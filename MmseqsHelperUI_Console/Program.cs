using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MmseqsHelperUI_Console;

namespace MmseqsHelperUI_Console;

internal sealed class Program
{
    public const string DefaultEnvVarPrefix = "MMSEQS_HELPER_";
    public const string DefaultConfigFileName = "mmseqs_helper_config.json";
    public static readonly List<string> HelpArgs = new List<string> { "h", "-h", "--h", "help", "-help", "--help" };

    private static async Task Main(string[] args)
    {
        Console.WriteLine($"Mmseqs Helper starting.");

        var allowedModes = new List<MmseqsHelperMode>()
        {
            // verb string is what the command line expects, process is what will be used to select the processing in code
            new MmseqsHelperModeGenerateMonoDbs("auto-mono"),
            new MmseqsHelperModeGenerateA3mFilesForColabfold("auto-pair"),
        };

        var configs = new List<string> { DefaultConfigFileName };

        //if (!args.Any())
        //{
        //    Console.WriteLine(selectedMode.GetHelpString(DefaultEnvVarPrefix, DefaultConfigFileName));
        //    return;
        //}

        var selectedMode = allowedModes.SingleOrDefault(x => x.VerbString == args.FirstOrDefault()) ??
                           MmseqsHelperMode.Null;

        // help fallback
        if (!args.Any() || args.Any(x => HelpArgs.Contains(x)))
        {
            Console.WriteLine(selectedMode.GetHelpString(DefaultEnvVarPrefix, DefaultConfigFileName));
            return;
        }

        // auto-generation of config file if needed
        if (args.Any(x => x.Equals("config", StringComparison.OrdinalIgnoreCase)))
        {
            if (File.Exists(DefaultConfigFileName))
            {
                Console.WriteLine($"Default config {DefaultConfigFileName} already exists.");
            }
            else
            {
                await File.WriteAllTextAsync(DefaultConfigFileName,
                    Helper.GetConfigJsonFromDefaults(selectedMode.GetDefaults()));
                Console.WriteLine($"Default config {DefaultConfigFileName} created.");
            }

            return;
        }

        // add configs 
        var jsons = args.Where(x => x.EndsWith(".json", StringComparison.OrdinalIgnoreCase)).ToList();
        if (jsons.Any())
        {
            const int maxConfigs = 1;
            if (jsons.Count <= maxConfigs)
            {
                configs = jsons;
            }
            else
            {
                throw new ArgumentException($"Too many configuration files provided, {maxConfigs} allowed.");
            }
        }

        var defaultConfig = selectedMode.GetDefaults().Where(x => !x.Value.required)
            .Select(x => new KeyValuePair<string, string>(x.Key, x.Value.defaultValue)).ToList();

        var hostBuilder = SetUpHostBuilder(configs, args, defaultConfig);

        if (false)
            hostBuilder.ConfigureAppConfiguration(builder =>
            {
                if (selectedMode.Process == MmseqsAutoProcess.GenerateA3mFilesForColabfold)
                {
                    //builder.AddInMemoryCollection(new List<KeyValuePair<string, string>>()
                    //{
                    //    new("InputFastaPaths",@"C:\temp\mmseqs_test\input.fasta"),
                    //    new("PersistedMonoDatabasePaths",@"C:\temp\mmseqs_test\result_db"),
                    //    new("OutputPath",@"C:\temp\mmseqs_test\out" )
                    //});
                }
            });



        var host = hostBuilder.Build();
        var program = host.Services.GetRequiredService<MmseqsHelperService>();

#if DEBUG
        await program.ExecuteAsync(selectedMode);
        return;
#endif

        try
        {
            await program.ExecuteAsync(selectedMode);
        }
        catch (Exception ex)
        {
            var message =
                $"Something went wrong and the program didn't know how to automatically handle it. Info below:\n\n{ex.Message}";
            Console.WriteLine(message);
            return;
        }

    }



    private static IHostBuilder SetUpHostBuilder(List<string>? providedConfigPaths, string[] args,
        List<KeyValuePair<string, string>> defaultConfig)
    {
        var hostBuilder = Host.CreateDefaultBuilder(args);

        hostBuilder.ConfigureAppConfiguration(builder =>
        {
            builder.Sources.Clear();
            builder.AddInMemoryCollection(defaultConfig);
            providedConfigPaths?.ForEach(x => builder.AddJsonFile(x, optional: true, reloadOnChange: false));
            builder.AddEnvironmentVariables(DefaultEnvVarPrefix);
            builder.AddCommandLine(args);
        });


        hostBuilder.ConfigureServices(sv =>
        {
            sv.AddSingleton<MmseqsHelperService>();
        });

        hostBuilder.ConfigureLogging((context, cfg) =>
        {
#if DEBUG
            cfg.SetMinimumLevel(LogLevel.Trace);
#endif
            cfg.AddSimpleConsole(options =>
            {
                options.SingleLine = true;
                options.TimestampFormat = "[yyyy-MM-dd HH:mm:ss.fff]#";
                options.IncludeScopes = false;
            });
        });

        return hostBuilder;

    }
}



