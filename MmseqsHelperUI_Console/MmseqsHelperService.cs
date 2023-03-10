using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MmseqsHelperLib;

namespace MmseqsHelperUI_Console;

internal sealed class MmseqsHelperService
{
    private readonly ILogger<MmseqsHelper> _logger;
    private readonly IConfiguration _configuration;

    public MmseqsHelperService(ILogger<MmseqsHelper> logger, IConfiguration configuration)
    {
        _logger = logger;

        _configuration = configuration;
    }

    public async Task ExecuteAsync(MmseqsHelperMode mode)
    {
        _logger.LogInformation($"Running mmseqs helper, mode: {mode}");

        var configJson = Helper.GetConfigJsonFromConfig(_configuration);
        _logger.LogInformation("Using combined defaults and inputs below:\n" + configJson);

        var settings = new AutoMmseqsSettings();

        if (mode.Process == MmseqsAutoProcess.GenerateMonoDbs)
        {
            var reqInputs = mode.GetDefaults().Where(x => x.Value.required).Select(x => x.Key);

            var missing = reqInputs.Where(x => _configuration[x] is null).ToList();

            if (missing.Any())
            {
                var message = $"Some required inputs missing, cannot continue. Missing inputs listed below:\n{String.Join(" ; ", missing)}";
                _logger.LogError(message);
                throw new ArgumentException(message);
            }

            settings.Custom.Add("UniprotDbPath", _configuration["UniprotDbPath"]); //?? "/path/to/uniprotdb");
            settings.Custom.Add("EnvDbPath", _configuration["EnvDbPath"]); //?? "/path/to/envdb");
            settings.MmseqsBinaryPath = _configuration["MmseqsBinaryPath"]; //?? "/path/to/binary/mmseqs";
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

            var a = new MmseqsHelperLib.MmseqsHelper(settings, _logger);
            await a.AutoCreateColabfoldMonoDbsFromFastasAsync(inputFastaPaths, dbPaths, excludedIds, outPath);
        }

    }

    


}