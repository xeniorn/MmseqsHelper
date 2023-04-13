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
        Strategy = new CalculationStrategy() {SuspiciousData = SuspiciousDataStrategy.PlaySafe};

        _logger.LogInformation($"Running mmseqs helper, mode: {mode}");

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

        var settings = new AutoColabfoldMmseqsSettings() {Strategy =  Strategy};
        var mmseqsSettings = GetMmseqsSettings(_configuration);
        
        var mmseqsHelper = new MmseqsHelper(mmseqsSettings, _logger);

        if (mode.Process == MmseqsAutoProcess.GenerateMonoDbs)
        {
            settings.Custom.Add("UniprotDbPath", _configuration["UniprotDbPath"]); //?? "/path/to/uniprotdb");
            settings.Custom.Add("EnvDbPath", _configuration["EnvDbPath"]); //?? "/path/to/envdb");
            settings.TempPath = _configuration["TempPath"]; //?? "/path/to/temp";

            var inputFastaPaths = _configuration["InputFastaPaths"]?.Split(',') ?? Array.Empty<string>();
            var dbPaths = _configuration["ExistingDatabasePaths"]?.Split(',') ?? Array.Empty<string>();
            var outPath = _configuration["OutputPath"]; //?? string.Empty;
            var excludedIdsFilePath = _configuration["ExclusionFilePath"] ?? string.Empty;
            
            List<string> excludedIds = new List<string>();

            if (File.Exists(excludedIdsFilePath))
            {
                excludedIds = (await File.ReadAllLinesAsync(excludedIdsFilePath)).Select(x => x.Trim()).ToList();
            }

            var a = new MmseqsHelperLib.ColabfoldMmseqsHelper(settings, _logger, mmseqsHelper);
            await a.GenerateColabfoldMonoDbsFromFastasAsync(inputFastaPaths, dbPaths, excludedIds, outPath);
        }
        else if (mode.Process == MmseqsAutoProcess.GenerateA3mFilesForColabfold)
        {
            settings.Custom.Add("UniprotDbPath", _configuration["UniprotDbPath"]); //?? "/path/to/uniprotdb");
            settings.Custom.Add("EnvDbPath", _configuration["EnvDbPath"]); //?? "/path/to/envdb");
            settings.TempPath = _configuration["TempPath"]; //?? "/path/to/temp";

            var inputFastaPaths = _configuration["InputFastaPaths"]?.Split(',') ?? Array.Empty<string>();
            var dbPaths = _configuration["ExistingDatabasePaths"]?.Split(',') ?? Array.Empty<string>();
            var a3mPaths = _configuration["ExistingA3mPaths"]?.Split(',') ?? Array.Empty<string>();
            var outPath = _configuration["OutputPath"]; //?? string.Empty;
            var excludedIdsFilePath = _configuration["ExclusionFilePath"] ?? string.Empty;

            List<string> excludedIds = new List<string>();

            if (File.Exists(excludedIdsFilePath))
            {
                excludedIds = (await File.ReadAllLinesAsync(excludedIdsFilePath)).Select(x => x.Trim()).ToList();
            }

            var a = new MmseqsHelperLib.ColabfoldMmseqsHelper(settings, _logger, mmseqsHelper);
            await a.GenerateA3msFromFastasGivenExistingMonoDbsAsync(inputFastaPaths, dbPaths, excludedIds, outPath, a3mPaths);
        }

    }

    public CalculationStrategy Strategy { get; set; }

    private MmseqsSettings GetMmseqsSettings(IConfiguration configuration)
    {
        var settings = new MmseqsSettings();

        settings.MmseqsBinaryPath = configuration["MmseqsBinaryPath"]; //?? "/path/to/binary/mmseqs";
        settings.TempPath = configuration["TempPath"]; //?? "/path/to/temp";
        
        if (int.TryParse(configuration["ThreadsPerMmseqsProcess"], out var parsedThreadCount))
        {
            settings.ThreadCount = parsedThreadCount;
        }
        else
        {
            _logger.LogError($"Failed to parse ThreadsPerMmseqsProcess, value should be an integer, was ({configuration["ThreadsPerMmseqsProcess"]})");
            if (Strategy.SuspiciousData == SuspiciousDataStrategy.PlaySafe) throw new ArgumentException();
        }

        
        if (Helper.TryParseBool(configuration["PreLoadDb"], out var parsedPreLoadDb))
        {
            settings.PreLoadDb = parsedPreLoadDb;
        }
        else
        {
            _logger.LogError($"Failed to parse PreLoadDb, value should be a bool, was ({configuration["PreLoadDb"]})");
            if (Strategy.SuspiciousData == SuspiciousDataStrategy.PlaySafe) throw new ArgumentException();
        }
        

        return settings;
    }
}