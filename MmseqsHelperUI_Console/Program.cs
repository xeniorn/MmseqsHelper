using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MmseqsHelperLib;

namespace MmseqsHelperUI_Console
{
    internal sealed class Program
    {
        private static async Task Main(string[] args)
        {

            Console.WriteLine("Hello World!");

            var inputFastaPaths = new List<string>() { @"C:\temp\mmseqs_test\input.fasta" };
            var dbPaths = new List<string>() { @"C:\temp\mmseqs_test\result_db" };
            var outPath = @"C:\temp\mmseqs_test\out";

            var defSettings = MmseqsHelper.GetDefaultSettings();
            defSettings.Custom.Add("UniprotDbPath", "/resources/uniprotdb");
            defSettings.Custom.Add("EnvDbPath", "/resources/envdb");
            defSettings.MmseqsBinaryPath = "/path/to/binary/mmseqs";
            defSettings.TempPath = @"C:\\temp\\mmseqs_test";

            var a = new MmseqsHelperLib.MmseqsHelper(defSettings, null);

            await a.AutoProcessFastaWithAutoIdFromSequenceForAlphaFoldAsync(inputFastaPaths, dbPaths, outPath);

            return;

            await Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddHostedService<MmseqsHelperService>();
                }).
            RunConsoleAsync();
            
        }
    }

    internal sealed class MmseqsHelperService : IHostedService
    {
        private readonly ILogger<MmseqsHelper> _logger;
        private readonly IHostApplicationLifetime _appLifetime;

        public MmseqsHelperService(
            ILogger<MmseqsHelper> logger,
            IHostApplicationLifetime appLifetime)
        {
            _logger = logger;
            _appLifetime = appLifetime;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug($"Starting with arguments: {string.Join(" ", Environment.GetCommandLineArgs())}");

            _appLifetime.ApplicationStarted.Register(() =>
            {
                Task.Run(async () =>
                {

                    _logger.LogInformation("Hello World!");

                    var inputFastaPaths = new List<string>() { @"C:\temp\mmseqs_test\input.fasta" };
                    var dbPaths = new List<string>() { @"C:\temp\mmseqs_test\result_db" };
                    var outPath = @"C:\temp\mmseqs_test\out";

                    var defSettings = MmseqsHelper.GetDefaultSettings();
                    defSettings.Custom.Add("UniprotDbPath", "/resources/uniprotdb");
                    defSettings.Custom.Add("EnvDbPath", "/resources/envdb");

                    var a = new MmseqsHelperLib.MmseqsHelper(defSettings, _logger);

                    await a.AutoProcessFastaWithAutoIdFromSequenceForAlphaFoldAsync(inputFastaPaths, dbPaths, outPath);

                    try
                    {
                        //_logger.LogInformation("Hello World!");

                        //var inputFastaPaths = new List<string>() { @"C:\temp\mmseqs_test\input.fasta" };
                        //var dbPaths = new List<string>() { @"C:\temp\mmseqs_test\result_db" };
                        //var outPath = @"C:\temp\mmseqs_test\out";

                        //var defSettings = MmseqsHelper.GetDefaultSettings();
                        //defSettings.Custom.Add("UniprotDbPath", "/resources/uniprotdb");
                        //defSettings.Custom.Add("EnvDbPath", "/resources/envdb");

                        //var a = new MmseqsHelperLib.MmseqsHelper(defSettings, _logger);

                        await a.AutoProcessFastaWithAutoIdFromSequenceForAlphaFoldAsync(inputFastaPaths, dbPaths, outPath);
                        
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unhandled exception!");
                    }
                    finally
                    {
                        // Stop the application once the work is done
                        _appLifetime.StopApplication();
                    }
                });
            });

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

}