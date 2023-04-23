using System.Net.NetworkInformation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MmseqsHelperLib;

namespace MmseqsHelperUI_Console;

internal sealed class MmseqsHelperService
{
    private readonly ILogger<ColabfoldMmseqsHelper> _logger;
    private readonly IConfiguration _configuration;

    public MmseqsHelperService(ILogger<ColabfoldMmseqsHelper> logger, IConfiguration configuration)
    {
        _logger = logger;

        _configuration = configuration;
    }

    public async Task ExecuteAsync(MmseqsHelperMode mode)
    {
        Strategy = GetStrategy();

        _logger.LogInformation($"Running mmseqs helper, mode: {mode} version: {ColabfoldMmseqsHelper.HardcodedColabfoldMmseqsHelperDatabaseVersion}");
        var configJson = Helper.GetConfigJsonFromConfig(_configuration);
        _logger.LogInformation("Using combined defaults and inputs below:\n" + configJson);

        var reqInputs = mode.GetDefaults().Where(x => x.Value.required).Select(x => x.Key);
        var missing = reqInputs.Where(x => _configuration[x] is null).ToList();
        if (missing.Any())
        {
            var message = $"Some required inputs missing, cannot continue. Missing inputs listed below:\n{String.Join(" ; ", missing)}";
            _logger.LogError(message);
            throw new ArgumentException(message);
        }
        
        var searchDbConfig = GetDbTargetConfig();
        var colabfoldMmseqsParamsConfig = GetColabfoldMmseqsParametersConfig();
        var colabfoldMmseqsParamsConfigRefDb = GetColabfoldMmseqsParametersConfigForUnpairedReferenceDb();
        var persistedA3MDbConfig = GetPersistedA3mConfig();
        var persistedMonoDbConfig = GetPersistedMonoConfig();
        var computingConfig = GetComputingConfig();
        

        var settings = new ColabfoldMmseqsHelperSettings(
            searchDatabasesConfig: searchDbConfig,
            persistedA3MDbConfig: persistedA3MDbConfig,
            persistedMonoDbConfig: persistedMonoDbConfig,
            strategy: Strategy, 
            computingConfig: computingConfig,
            colabfoldMmseqsParams: colabfoldMmseqsParamsConfig,
            colabfoldMmseqsParamsUnpairedSpecialForReferenceDb: colabfoldMmseqsParamsConfigRefDb
            );

        var temp = _configuration["TempPath"]!;
        await MmseqsHelperLib.Helper.CreateDirectoryAsync(temp);
        File.WriteAllText(Path.Join(temp,"allSettingsDump.json"), settings.ToJson());
        
        switch (mode.Process)
        {
            case MmseqsAutoProcess.GenerateMonoDbs:
                {
                    var inputFastaPaths = _configuration["InputFastaPaths"]?.Split(',') ?? Array.Empty<string>();
                    var dbPaths = _configuration["PersistedMonoDatabasePaths"]?.Split(',') ?? Array.Empty<string>();
                    var outPath = _configuration["OutputPath"]; //?? string.Empty;
                    var excludedIdsFilePath = _configuration["ExclusionFilePath"] ?? string.Empty;

                    List<string> excludedIds = new List<string>();

                    if (File.Exists(excludedIdsFilePath))
                    {
                        excludedIds = (await File.ReadAllLinesAsync(excludedIdsFilePath)).Select(x => x.Trim()).ToList();
                    }

                    var a = new ColabfoldMmseqsHelper(settings, _logger);
                    await a.GenerateColabfoldMonoDbsFromFastasAsync(inputFastaPaths, dbPaths, excludedIds, outPath);
                    break;
                }
            case MmseqsAutoProcess.GenerateA3mFilesForColabfold:
                {
                    var inputFastaPaths = _configuration["InputFastaPaths"]?.Split(',') ?? Array.Empty<string>();
                    var dbPaths = _configuration["PersistedMonoDatabasePaths"]?.Split(',') ?? Array.Empty<string>();
                    var a3mPaths = _configuration["PersistedA3mResultDatabasePaths"]?.Split(',') ?? Array.Empty<string>();
                    var outPath = _configuration["OutputPath"]; //?? string.Empty;
                    var excludedIdsFilePath = _configuration["ExclusionFilePath"] ?? string.Empty;

                    List<string> excludedIds = new List<string>();

                    if (File.Exists(excludedIdsFilePath))
                    {
                        excludedIds = (await File.ReadAllLinesAsync(excludedIdsFilePath)).Select(x => x.Trim()).ToList();
                    }

                    var a = new ColabfoldMmseqsHelper(settings, _logger);
                    await a.GenerateA3msFromFastasGivenExistingMonoDbsAsync(inputFastaPaths, dbPaths, excludedIds, outPath, a3mPaths);
                    break;
                }

            case MmseqsAutoProcess.MimicColabfoldSearch:
                {
                    var inputFastaPaths = _configuration["InputFastaPaths"]?.Split(',') ?? Array.Empty<string>();
                    var dbPathRoot = _configuration["PersistedResultsPath"] ?? string.Empty;
                    var monoPath = Path.Join(dbPathRoot, "mono");
                    var a3mPath = Path.Join(dbPathRoot, "a3m");
                    var outPath = _configuration["OutputPath"]; //?? string.Empty;

                    var dirCreateTasks = new List<Task>()
                    {
                        MmseqsHelperLib.Helper.CreateDirectoryAsync(monoPath),
                        MmseqsHelperLib.Helper.CreateDirectoryAsync(a3mPath),
                        MmseqsHelperLib.Helper.CreateDirectoryAsync(outPath),
                    };

                    await Task.WhenAll(dirCreateTasks);

                    var a = new ColabfoldMmseqsHelper(settings, _logger);

                    _logger.LogInformation("Starting mono db search followed by final a3m generation for multimers.");
                    _logger.LogInformation("Starting a3m generation for existing mono results in parallel, if any exist.");

                    var tasks = new List<Task>()
            {
                a.GenerateColabfoldMonoDbsFromFastasAsync(
                    inputFastaPaths, new[] {monoPath}, Array.Empty<string>(), monoPath),
                a.GenerateA3msFromFastasGivenExistingMonoDbsAsync(
                    inputFastaPaths, new[] {monoPath}, Array.Empty<string>(), a3mPath, new[] {a3mPath}),
            };

                    await Task.WhenAll(tasks);

                    _logger.LogInformation("Redoing a3m generation for any missing ones...");

                    await a.GenerateA3msFromFastasGivenExistingMonoDbsAsync(
                        inputFastaPaths, new[] { monoPath }, Array.Empty<string>(), a3mPath, new[] { a3mPath });

                    await a.GetExistingResultsFromDatabaseAndNameThemAsync(inputFastaPaths, new[] { a3mPath }, outPath);

                    break;
                }
            default:
                throw new NotImplementedException("This should never happen.");
                break;
        }
    }

    private TrackingStrategyConfiguration GetTrackingConfig()
    {
        var idStrategy = Helper.ParseEnumOrDefault<TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy>(_configuration["ComputerIdentificationStrategy"],
            TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.HostName);
        

        var a = new TrackingStrategyConfiguration()
        {
            ComputerIdentifierSource = idStrategy
        };

        return a;

    }

    private IssueHandlingStrategy GetStrategy()
    {
        var a = new IssueHandlingStrategy()
            { SuspiciousData = SuspiciousDataStrategy.PlaySafe, AllowDifferentMmseqsVersion = true };
        return a;
    }

    private ComputingStrategyConfiguration GetComputingConfig()
    {
        const int monoBatchSizeHardcodedDefault = 500;
        const int a3mBatchSizeHardcodedDefault = 1000;
        const int existingDbSearchParallelizationHardcodedDefault = 20;

        var trackingConfig = GetTrackingConfig();


        var a = new ComputingStrategyConfiguration()
        {
            MaxDesiredMonoBatchSize = Helper.ParseIntOrDefault(_configuration["SearchMaxBatchSize"], monoBatchSizeHardcodedDefault),
            MaxDesiredPredictionTargetBatchSize = Helper.ParseIntOrDefault(_configuration["PairingMaxBatchSize"], a3mBatchSizeHardcodedDefault),
            ExistingDatabaseSearchParallelizationFactor = Helper.ParseIntOrDefault(_configuration["ExistingDbSearchParallelization"], existingDbSearchParallelizationHardcodedDefault),
            MmseqsSettings = GetMmseqsSettings(),
            ReportSuccessfulUsageOfPersistedDb = true,
            DeleteTemporaryData = Helper.ParseBoolOrDefault(_configuration["DeleteTemporaryData"], true),
            TempPath = _configuration["TempPath"],
            TrackingConfig = trackingConfig,
    };
        
        return a;

    }

    

    private PersistedMonoDatabaseConfiguration GetPersistedMonoConfig()
    {
        return new PersistedMonoDatabaseConfiguration()
        {
            LastAccessReporterFilename = "LAST_USED"
    };
    }

    private PersistedA3mDatabaseConfiguration GetPersistedA3mConfig()
    {
        return new PersistedA3mDatabaseConfiguration();
    }

    private ColabfoldMmseqsParametersConfiguration GetColabfoldMmseqsParametersConfig()
    {
        return new ColabfoldMmseqsParametersConfiguration()
        {
            Search = ColabfoldMmseqsHelperSettings.colabFold_SearchParamsShared,
            Unpaired = new()
            {
                Expand = ColabfoldMmseqsHelperSettings.colabFold_ExpandParamsEnvMono,
                Align = ColabfoldMmseqsHelperSettings.colabFold_AlignParamsMono,
                Filter = ColabfoldMmseqsHelperSettings.colabFold_FilterParams,
                MsaConvert = ColabfoldMmseqsHelperSettings.colabFold_MsaConvertParamsMono
            },
            Paired = new ()
            {
                Expand = ColabfoldMmseqsHelperSettings.colabFold_ExpandParamsUnirefPair,
                Align1 = ColabfoldMmseqsHelperSettings.colabFold_Align1ParamsPair,
                Align2 = ColabfoldMmseqsHelperSettings.colabFold_Align2ParamsPair,
                MsaConvert = ColabfoldMmseqsHelperSettings.colabFold_MsaConvertParamsPair
            }
        };
    }

    private ColabfoldMmseqsUnpairedParametersConfiguration GetColabfoldMmseqsParametersConfigForUnpairedReferenceDb()
    {
        return new ColabfoldMmseqsUnpairedParametersConfiguration()
        {
            Expand = ColabfoldMmseqsHelperSettings.colabFold_ExpandParamsUnirefMono,
            Align = ColabfoldMmseqsHelperSettings.colabFold_AlignParamsMono,
            Filter = ColabfoldMmseqsHelperSettings.colabFold_FilterParams,
            MsaConvert = ColabfoldMmseqsHelperSettings.colabFold_MsaConvertParamsMono
        }; 
    }

    private SearchDatabasesConfiguration GetDbTargetConfig()
    {
        //TODO: unhack it
        var preloadToRam = Helper.ParseBoolOrDefault(_configuration["UseRamPreloading"], false);
        var usePairing = Helper.ParseBoolOrDefault(_configuration["UsePairing"], true);
        var useEnvDb = Helper.ParseBoolOrDefault(_configuration["UseEnv"], true);

        var uniprotDbPath = _configuration["UniprotDbPath"];
        var envDbPath = _configuration["EnvDbPath"];

        var uniprotDbName = Path.GetFileName(uniprotDbPath);
        var envDbName = Path.GetFileName(envDbPath);

        var uniprotDb = new MmseqsSourceDatabase(uniprotDbName, uniprotDbPath, new MmseqsSourceDatabaseFeatures(HasTaxonomyData: true));
        var envDb = new MmseqsSourceDatabase(envDbName, envDbPath, new MmseqsSourceDatabaseFeatures(HasTaxonomyData: false));

        //TODO: definitely move this to external config asap. we can't be initializing stuff hardcoded
        var uniprotDbTarget = new MmseqsSourceDatabaseTarget(uniprotDb, true, usePairing, preloadToRam);
        var envDbTarget = new MmseqsSourceDatabaseTarget(envDb, true, false, preloadToRam);

        var refDbTarget = uniprotDbTarget;

        var dbTargets = new List<MmseqsSourceDatabaseTarget>()
        {
            uniprotDbTarget
        };

        if (useEnvDb)
        {
            dbTargets.Add(envDbTarget);
        }

        return new SearchDatabasesConfiguration()
        {
            DbTargets = dbTargets,
            ReferenceDbTarget = refDbTarget
        };

    }

    public IssueHandlingStrategy Strategy { get; set; }

    private MmseqsSettings GetMmseqsSettings()
    {
        // doesn't work, would have to be made mutable... don't want that
        // TODO: jsondeserialize instead?
        // var intSet = new MmseqsInternalConfigurationSettings();
        // _configuration.GetSection(MmseqsInternalConfigurationSettings.ConfigurationName).Bind(intSet);
        
        var settings = new MmseqsSettings();

        settings.MmseqsBinaryPath = _configuration["MmseqsBinaryPath"]; //?? "/path/to/binary/mmseqs";
        // settings.TempPath = configuration["TempPath"]; //?? "/path/to/temp";
        
        if (int.TryParse(_configuration["ThreadsPerMmseqsProcess"], out var parsedThreadCount))
        {
            settings.ThreadsPerProcess = parsedThreadCount;
        }
        else
        {
            _logger.LogError($"Failed to parse ThreadsPerMmseqsProcess, value should be an integer, was ({_configuration["ThreadsPerMmseqsProcess"]})");
            if (Strategy.SuspiciousData == SuspiciousDataStrategy.PlaySafe) throw new ArgumentException();
        }
        
        if (Helper.TryParseBool(_configuration["UsePrecalculatedIndex"], out var parsedPrecalcualtedIndex))
        {
            settings.UsePrecalculatedIndex = parsedPrecalcualtedIndex;
        }
        else
        {
            _logger.LogError($"Failed to parse UsePrecalculatedIndex, value should be a bool, was ({_configuration["UsePrecalculatedIndex"]})");
            if (Strategy.SuspiciousData == SuspiciousDataStrategy.PlaySafe) throw new ArgumentException();
        }


        return settings;
    }
}