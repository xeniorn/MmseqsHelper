using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace MmseqsHelperLib
{
    public class MmseqsHelper
    {
        public string MmseqsBinaryPath { get; set; }


        public async Task TestAsync()
        {
            var inputFasta = "input.fasta";
            //var contents = File.ReadAllText(inputFasta);
            var id = GetMd5Hash(inputFasta);
            var outputDbNameBase = "id_qdb";

            var inputPaths = new List<string>() { inputFasta };

            var createDbParameters = new CreateDbParameters();
            
            var createDbTask = CreateDb(inputPaths, outputDbNameBase, createDbParameters);

            await createDbTask;

        }

        private async Task CreateDb(IEnumerable<string> inputPaths, string outputDbNameBase, CreateDbParameters parameters)
        {
            var command = parameters.CommandString;                       

            var positionalArguments = inputPaths.Append(outputDbNameBase);

            await RunMmseqs(command, positionalArguments, parameters);
        }

        private async Task RunMmseqs(string mmseqsModule, IEnumerable<string> positionalArguments, MmseqsCommandLineParameters parameters)
        {
            var fullFilePath = EnsureQuotedIfWhiteSpace(MmseqsBinaryPath);
            var positionalArgumentsString = String.Join(" ", positionalArguments.Select(EnsureQuotedIfWhiteSpace));
            var parametersString = String.Join(" ", parameters.GetNonDefault().Select(x => x.GetCommandLineString()));

            var processArgumentsString = $"{mmseqsModule} {positionalArgumentsString} {parametersString}";

            //var fullCommand = $"{mmseqsCommandString} {positionalArgumentsString} {parametersString}";

            var x = await RunProcessAsync(fullFilePath, processArgumentsString);

        }

        private static string EnsureQuotedIfWhiteSpace(string input)
        {
            if (!HasWhitespace(input)) return input;
            return IsQuoted(input) ? input : $"\"{input}\"";
        }
        private static bool HasWhitespace(string input) => input.Any(x => Char.IsWhiteSpace(x));
        private static bool IsQuoted(string input) => input.Length > 2 && input.First() == '"' && input.Last() == '"';


        private string GetMd5Hash(string source)
        {
            using System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create();

            var inputBytes = Encoding.ASCII.GetBytes(source);
            var hashBytes = md5.ComputeHash(inputBytes);

            return Convert.ToHexString(hashBytes);

        }


        public static async Task<int> RunProcessAsync(string fileName, string args)
        {
            using (var process = new Process
                   {
                       StartInfo =
                       {
                           FileName = fileName, Arguments = args,
                           UseShellExecute = false, CreateNoWindow = true,
                           RedirectStandardOutput = true, RedirectStandardError = true
                       },
                       EnableRaisingEvents = true
                   })
            {
                return await RunProcessAsync(process).ConfigureAwait(false);
            }
        }
        private static Task<int> RunProcessAsync(Process process)
        {
            var tcs = new TaskCompletionSource<int>();

            process.Exited += (s, ea) => tcs.SetResult(process.ExitCode);
            process.OutputDataReceived += (s, ea) => Console.WriteLine(ea.Data);
            process.ErrorDataReceived += (s, ea) => Console.WriteLine("ERR: " + ea.Data);

            bool started = process.Start();
            if (!started)
            {
                //you may allow for the process to be re-used (started = false) 
                //but I'm not sure about the guarantees of the Exited event in such a case
                throw new InvalidOperationException("Could not start process: " + process);
            }

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            return tcs.Task;
        }

    }

    internal abstract class MmseqsCommandLineParameters : CommandLineParameters
    {
        public abstract string CommandString { get; }
    }

    internal class CreateDbParameters : MmseqsCommandLineParameters
    {
        public CreateDbParameters()
        {
            var defaultParams = new List<ICommandLineParameter>()
            {
                new CommandLineParameter<int>("--dbtype", "Database type 0: auto, 1: amino acid 2: nucleotides", 0, new List<CommandLineFeature>() 
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
                new CommandLineParameter<bool>("--shuffle", "Shuffle input database", true, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
                new CommandLineParameter<int>("--createdb-mode", "Createdb mode 0: copy data, 1: soft link data and write new index (works only with single line fasta/q)", 0, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),                
                new CommandLineParameter<int>("--id-offset", "Numeric ids in index file are offset by this value", 0, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
                new CommandLineParameter<int>("--compressed", "Write compressed output [0]", 0, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
                new CommandLineParameter<int>("-v", "Verbosity level: 0: quiet, 1: +errors, 2: +warnings, 3: +info", 3, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace, CommandLineFeature.AffectsOnlyVerbosityOrSimilar}),
                new CommandLineParameter<int>("--write-lookup", "write .lookup file containing mapping from internal id, fasta id and file number [1]", 1, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace})
            };

            Parameters = defaultParams;
        }

        public override string CommandString => "createdb";
    }

    internal class CommandLineParameters
    {
        public CommandLineParameters()
        {
            this.Parameters = new List<ICommandLineParameter>();
        }

        public List<ICommandLineParameter> Parameters { get; protected init; }

        public IEnumerable<ICommandLineParameter> GetNonDefault()
        {
            return Parameters.Where(x=> x.DefaultValue.CompareTo(x.Value) != 0);
        }

    }
}