using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using FastaHelperLib;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MmseqsHelperLib
{
    public class MmseqsHelper
    {
        private readonly ILogger<MmseqsHelper> _logger;

        public MmseqsHelper(AutoMmseqsSettings? inputSettings = null, ILogger<MmseqsHelper> logger = null)
        {
            _logger = logger;
            Settings = inputSettings ?? GetDefaultSettings();
        }

        public AutoMmseqsSettings Settings { get; set; }

        public string MmseqsBinaryPath => Settings.MmseqsBinaryPath;


        public async Task AutoProcessFastaWithAutoIdFromSequenceForAlphaFoldAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, string outputPath)
        {
            #region HardcodedSetup
            const string colabFold_SearchParamsShared = @"--num-iterations 3 -a -s 8 -e 0.1 --max-seqs 10000";
            Settings.Custom.Add("colabFold_SearchParamsShared", colabFold_SearchParamsShared);
            
            const string colabFold_ExpandParamsUnirefMono =
                @"--expansion-mode 0 -e inf --expand-filter-clusters 1 --max-seq-id 0.95";
            Settings.Custom.Add("colabFold_ExpandParamsUnirefMono", colabFold_ExpandParamsUnirefMono);
            const string colabFold_ExpandParamsUnirefPair =
                @"--expansion-mode 0 -e inf --expand-filter-clusters 0 --max-seq-id 0.95";
            Settings.Custom.Add("colabFold_ExpandParamsUnirefPair", colabFold_ExpandParamsUnirefPair);
            const string colabFold_ExpandParamsEnvMono =
                @"--expansion-mode 0 -e inf";
            Settings.Custom.Add("colabFold_ExpandParamsEnvMono", colabFold_ExpandParamsEnvMono);
            
            const string colabFold_FilterParams =
                @"--qid 0 --qsc 0.8 --diff 0 --max-seq-id 1.0 --filter-min-enable 100";
            Settings.Custom.Add("colabFold_FilterParams", colabFold_FilterParams);
            
            const string colabFold_AlignParamsMono = @"-e 10  --max-accept 1000000 --alt-ali 10 -a";
            Settings.Custom.Add("colabFold_AlignParamsMono", colabFold_AlignParamsMono);
            const string colabFold_AlignParamsPair = @"-e 0.001  --max-accept 1000000 -c 0.5 --cov-mode 1 -a";
            Settings.Custom.Add("colabFold_AlignParamsPair", colabFold_AlignParamsPair);
            
            const string colabFold_MsaConvertParamsMono = @"--msa-format-mode 6 --filter-msa 1 --filter-min-enable 1000 --diff 3000 --qid '0.0,0.2,0.4,0.6,0.8,1.0' --qsc 0 --max-seq-id 0.95";
            Settings.Custom.Add("colabFold_MsaConvertParamsMono", colabFold_MsaConvertParamsMono);
            const string colabFold_MsaConvertParamsPair = @"--msa-format-mode 5";
            Settings.Custom.Add("colabFold_MsaConvertParamsPair", colabFold_MsaConvertParamsPair);

            var performanceParams = @$"--threads {Settings.ThreadCount} --db-load-mode {(Settings.PreLoadDb ? 2 : 0)}";
            Settings.Custom.Add("performanceParams", performanceParams);
            #endregion

            var (existingRes, missingRes) = await GetExistingAndMissingSetsAsync(inputFastaPaths, existingDatabasePaths);
            
            var batches = GetBatches(missingRes);
                        
            foreach (var proteinBatch in batches)
            {
                var batchIdBase = Guid.NewGuid().ToString();
                var batchId = $"{batchIdBase}";

                var workingDir = Path.Join(Settings.TempPath, batchId);
                Directory.CreateDirectory(workingDir);

                //*******************************************create source fasta file*******************************************************
                var fastaName = $"input{Settings.FastaSuffix}";
                var queryFastaPath = Path.Join(workingDir, fastaName);
                using (var fastaFileOutputStream = File.Create(queryFastaPath))
                {
                    await foreach (var fastaChunk in FastaHelper.GenerateMultiFastaDataAsync(proteinBatch))
                    {
                        await fastaFileOutputStream.WriteAsync(Encoding.ASCII.GetBytes(fastaChunk));
                    }
                }

                //*******************************************create query db file*******************************************************
                var qdbNameBase = $"{Settings.QdbSuffix}";
                var qdbPath = Path.Join(workingDir, qdbNameBase);

                var createDbParameters = new CreateDbParameters();
                createDbParameters.ApplyDefaults();
                await CreateDbAsync(queryFastaPath, qdbPath, createDbParameters);

                //*******************************************initial uniprot search to get search & profile dbs****************************
                var (searchDb, profileDb) = await Task.Run(() => AutoUniprotSearchAndProfileAsync(workingDir, qdbPath));
                
                //*******************************************calc mono and pair dbs*******************************************************
                var uniprotMonoTask = Task.Run(() => AutoUniprotMonoLineAsync(workingDir, qdbPath, searchDb, profileDb));
                var uniprotPairTask = Task.Run(() => AutoUniprotPairLineAsync(workingDir, qdbPath, searchDb));
                var envDbMonoTask = Task.Run(()=> AutoEnvDbLineAsync(workingDir, qdbPath, profileDb));

                await Task.WhenAll(uniprotMonoTask, uniprotPairTask, envDbMonoTask);

                var monoDbPath = uniprotMonoTask.Result;
                var pairDbPath = uniprotPairTask.Result;
                var envDbPath = envDbMonoTask.Result;

                (string mergedMonoResult, string pairDbResult) = await AutoFinalResultsAsync(workingDir, qdbPath, monoDbPath, pairDbPath, envDbPath);
                
            }
        }

        private async Task<(string searchDb, string profileDb)> AutoUniprotSearchAndProfileAsync(string workingDir, string qdbPath)
        {
            //*******************************************search*******************************************************
            const string searchModule = @"search";
            var processingFolderRoot = Path.Join(workingDir, "uniprot_shared");
            Directory.CreateDirectory(processingFolderRoot);
            var tempSubfolderForUniprotSearch = Path.Join(processingFolderRoot, "tmp");
            Directory.CreateDirectory(tempSubfolderForUniprotSearch);
            var expectedGeneratedProfileSubPath = Path.Join("latest", "profile_1");

            var searchResultDb = Path.Join(processingFolderRoot, $"search");
            var searchPosParams = new List<string>() { qdbPath, Settings.Custom["UniprotDbPath"], searchResultDb, tempSubfolderForUniprotSearch };
            await RunMmseqsAsync(searchModule, searchPosParams, $"{Settings.Custom["colabFold_SearchParamsShared"]} {Settings.Custom["performanceParams"]}");

            //*******************************************hack up a profile db*******************************************************
            var profileResultDbOriginal = Path.Join(tempSubfolderForUniprotSearch, expectedGeneratedProfileSubPath);
            var profileResultDb = Path.Join(processingFolderRoot, "profile");

            //***move temp file from search as profile db***
            const string moveModule = @"mvdb";
            var movePosParams = new List<string>() { profileResultDbOriginal, profileResultDb };
            await RunMmseqsAsync(moveModule, movePosParams, String.Empty);

            //***link to header db of qdb since it has the same values***
            const string linkModule = @"lndb";
            var linkPosParams = new List<string>() { qdbPath + Settings.Mmseqs2Internal_DbHeaderSuffix, profileResultDb + Settings.Mmseqs2Internal_DbHeaderSuffix };
            await RunMmseqsAsync(linkModule, linkPosParams, String.Empty);

            return (searchResultDb, profileResultDb);

        }

        private async Task<(string mergedMonoResult, string pairDbResult)> AutoFinalResultsAsync(string workingDir, string qdbPath, string uniprotMonoDb, string uniprotPairDb, string envDbMonoDb)
        {
            var localProcessingPath = Path.Join(workingDir, "final");

            //*******************************************merge the mono dbs*******************************************************
            const string mergeModule = @"mergedbs";
            var mergeMonoResultDb = Path.Join(localProcessingPath, $"final_mono.a3m");
            var mergePosParams = new List<string>()
            {
                qdbPath,
                mergeMonoResultDb,
                uniprotMonoDb,
                envDbMonoDb
            };
            await RunMmseqsAsync(mergeModule, mergePosParams, String.Empty);

            //*******************************************move the result  files to final output*************************************



            return (mergeMonoResultDb, uniprotPairDb);
        }

        private async Task<string> AutoEnvDbLineAsync(string workingDir, string qdbPath, string profileResultDb)
        {
            var localProcessingPath = Path.Join(workingDir, "env_mono");
            Directory.CreateDirectory(localProcessingPath);
            var targetDbPathBase = Settings.Custom["EnvDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
            var targetDbPathAln = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

            //*******************************************search*******************************************************
            const string searchModule = @"search";
            var tempSubfolderForSearch = Path.Join(localProcessingPath, "tmp");
            Directory.CreateDirectory(tempSubfolderForSearch);

            var searchResultDb = Path.Join(localProcessingPath, $"search");
            var searchPosParams = new List<string>()
            {
                qdbPath, 
                targetDbPathBase, 
                searchResultDb,
                tempSubfolderForSearch
            };
            await RunMmseqsAsync(searchModule, searchPosParams, $"{Settings.Custom["colabFold_SearchParamsShared"]} {Settings.Custom["performanceParams"]}");

            //*******************************************expand*******************************************************
            const string expandModule = @"expand";
            var expandResultDb = Path.Join(localProcessingPath, $"expand");
            var expandPosParams = new List<string>()
            {
                profileResultDb,
                targetDbPathSeq,
                searchResultDb,
                targetDbPathAln,
                expandResultDb
            };
            await RunMmseqsAsync(expandModule, expandPosParams, $"{Settings.Custom["colabFold_ExpandParamsEnvMono"]} {Settings.Custom["performanceParams"]}");

            //*******************************************align*******************************************************
            const string alignModule = @"align";
            var alignResultDb = Path.Join(localProcessingPath, $"align");
            var alignPosParams = new List<string>()
            {
                profileResultDb,
                targetDbPathSeq,
                expandResultDb,
                alignResultDb,
            };
            await RunMmseqsAsync(alignModule, alignPosParams, $"{Settings.Custom["colabFold_AlignParamsMono"]} {Settings.Custom["performanceParams"]}");

            //*******************************************filter*******************************************************
            const string filterModule = @"filterresult";
            var filterResultDb = Path.Join(localProcessingPath, $"filter");
            var filterPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                alignResultDb,
                filterResultDb,
            };
            await RunMmseqsAsync(filterModule, filterPosParams, $"{Settings.Custom["colabFold_FilterParams"]} {Settings.Custom["performanceParams"]}");

            //*******************************************convert*******************************************************
            const string msaConvertModule = @"result2msa";
            var msaConvertResultDb = Path.Join(localProcessingPath, $"env.a3m");
            var msaConvertPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                filterResultDb,
                msaConvertResultDb,
            };
            await RunMmseqsAsync(msaConvertModule, msaConvertPosParams, $"{Settings.Custom["colabFold_MsaConvertParamsMono"]} {Settings.Custom["performanceParams"]}");

            return msaConvertResultDb;
        }

        private async Task<string> AutoUniprotMonoLineAsync(string workingDir, string qdbPath, string searchResultDb, string profileResultDb)
        {
            var localProcessingPath = Path.Join(workingDir, "uniprot_mono");
            Directory.CreateDirectory(localProcessingPath);
            var targetDbPathBase = Settings.Custom["UniprotDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
            var targetDbPathAln = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

            //*******************************************expand*******************************************************
            const string expandModule = @"expand";
            var expandResultDb = Path.Join(localProcessingPath, $"expand");
            var expandPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                searchResultDb,
                targetDbPathAln,
                expandResultDb
            };
            await RunMmseqsAsync(expandModule, expandPosParams, $"{Settings.Custom["colabFold_ExpandParamsUnirefMono"]} {Settings.Custom["performanceParams"]}");

            //*******************************************align*******************************************************
            const string alignModule = @"align";
            var alignResultDb = Path.Join(localProcessingPath, $"align");
            var alignPosParams = new List<string>()
            {
                profileResultDb,
                targetDbPathSeq,
                expandResultDb,
                alignResultDb,
            };
            await RunMmseqsAsync(alignModule, alignPosParams, $"{Settings.Custom["colabFold_AlignParamsMono"]} {Settings.Custom["performanceParams"]}");

            //*******************************************filter*******************************************************
            const string filterModule = @"filterresult";
            var filterResultDb = Path.Join(localProcessingPath, $"filter");
            var filterPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                alignResultDb,
                filterResultDb,
            };
            await RunMmseqsAsync(filterModule, filterPosParams, $"{Settings.Custom["colabFold_FilterParams"]} {Settings.Custom["performanceParams"]}");

            //*******************************************convert*******************************************************
            const string msaConvertModule = @"result2msa";
            var msaConvertResultDb = Path.Join(localProcessingPath, $"uniref.a3m");
            var msaConvertPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                filterResultDb,
                msaConvertResultDb,
            };
            await RunMmseqsAsync(msaConvertModule, msaConvertPosParams, $"{Settings.Custom["colabFold_MsaConvertParamsMono"]} {Settings.Custom["performanceParams"]}");

            return msaConvertResultDb;

        }

        private async Task<string> AutoUniprotPairLineAsync(string workingDir, string qdbPath, string searchResultDb)
        {
            var localProcessingPath = Path.Join(workingDir, "uniprot_pair");
            Directory.CreateDirectory(localProcessingPath);
            var targetDbPathBase = Settings.Custom["UniprotDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
            var targetDbPathAln = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

            //*******************************************expand*******************************************************
            const string expandModule = @"expand";
            var expandResultDb = Path.Join(localProcessingPath, $"expand");
            var expandPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                searchResultDb,
                targetDbPathAln,
                expandResultDb
            };
            await RunMmseqsAsync(expandModule, expandPosParams, $"{Settings.Custom["colabFold_ExpandParamsUnirefPair"]} {Settings.Custom["performanceParams"]}");

            //*******************************************align*******************************************************
            const string alignModule = @"align";

            var alignResultDb = Path.Join(localProcessingPath, $"align");
            var alignPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                expandResultDb,
                alignResultDb,
                
            };
            await RunMmseqsAsync(alignModule, alignPosParams, $"{Settings.Custom["colabFold_AlignParamsPair"]} {Settings.Custom["performanceParams"]}");
            
            return "alignResultDb";
        }

        private async Task CreateDbAsync(string queryFastaPath, string outputDbNameBase, CreateDbParameters createDbParameters)
        {
            await CreateDbAsync(new List<String> { queryFastaPath }, outputDbNameBase, createDbParameters);
        }

        private List<List<Protein>> GetBatches(List<Protein> missingRes)
        {
            var batchCount = 1 + (missingRes.Count - 1) / Settings.MaxDesiredBatchSize ;

            var batches = new List<List<Protein>>();

            //https://stackoverflow.com/a/4262134/4554766
            var rng = new Random();
            var counter = 0;
            var tempList = new List<Protein>();
            foreach (var protein in missingRes.OrderBy(x=> rng.Next()))
            {
                tempList.Add(protein);
                counter++;
                if (counter == batchCount)
                {
                    batches.Add(tempList);
                    tempList.Clear();
                    counter = 0;
                }
            }

            // if there is an unfinished list, still add it
            if (tempList.Any())
            {
                batches.Add(tempList);
            }

            return batches;
        }

        private class ProteinByIdComparer : EqualityComparer<Protein>
        {
            public override bool Equals(Protein? x, Protein? y)
            {
                return x.Id.Equals(y.Id, StringComparison.InvariantCulture);
            }

            public override int GetHashCode(Protein obj)
            {
                return obj.GetHashCode();
            }
        }

        private async Task<(List<Protein> existing,List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths)
        {
            var uniqueInputFastaEntries = new HashSet<Protein>(new ProteinByIdComparer());
            
            foreach (var inputFastaPath in inputFastaPaths)
            {
                var stream = File.OpenRead(inputFastaPath);
                var fastas = await FastaHelper.GetFastaEntriesIfValidAsync(stream, SequenceType.Protein);
                if (fastas is not null && fastas.Any())
                {
                    foreach (var fastaEntry in fastas)
                    {
                        var protein = new Protein()
                            { Id = GetMd5Hash(fastaEntry.Sequence), Sequence = fastaEntry.Sequence };
                        uniqueInputFastaEntries.Add(protein);
                    }
                }
            }

            var existing = new List<Protein>();

            var useExistingImplemented = false;
            if (false)
            {
                foreach (var existingDatabasePath in existingDatabasePaths)
                {
                    var filesInThisPath = Directory.GetFiles(existingDatabasePath);

                    //TODO: not yet implemented

                    var qdbSets = new List<(string data, string dataIndex, string header, string headerIndex)>();
                    var indexFiles = filesInThisPath.Where(x =>
                        x.EndsWith($"{ Settings.QdbSuffix}${Settings.Mmseqs2Internal_DbHeaderSuffix}"));
                }

            }

            var missing = uniqueInputFastaEntries.Except(existing).ToList();

            return (existing, missing);

        }

        public static AutoMmseqsSettings GetDefaultSettings()
        {
            var settings = new AutoMmseqsSettings()
            {
            };
            return settings;
        }

        private async Task CreateDbAsync(IEnumerable<string> inputPaths, string outputDbNameBase, CreateDbParameters parameters)
        {
            var command = parameters.CommandString;
            var positionalArguments = inputPaths.Append(outputDbNameBase);
            await RunMmseqsAsync(command, positionalArguments, parameters);
        }

        private async Task RunMmseqsAsync(string mmseqsModule, IEnumerable<string> positionalArguments, MmseqsCommandLineParameters nonPositionalParameters)
        {
            var parametersString = String.Join(" ", nonPositionalParameters.GetNonDefault().Select(x => x.GetCommandLineString()));
            await RunMmseqsAsync(mmseqsModule, positionalArguments, parametersString);
        }

        private async Task RunMmseqsAsync(string mmseqsModule, IEnumerable<string> positionalArguments, string nonPositionalParametersString)
        {
            var fullFilePath = EnsureQuotedIfWhiteSpace(MmseqsBinaryPath);
            var positionalArgumentsString = String.Join(" ", positionalArguments.Select(EnsureQuotedIfWhiteSpace));
            var processArgumentsString = $"{mmseqsModule} {positionalArgumentsString} {nonPositionalParametersString}";

            LogSomething($"{fullFilePath} {processArgumentsString}");
            return;

            var exitCode = await RunProcessAsync(fullFilePath, processArgumentsString);
            
            const int successExit = 0;
            if (exitCode != successExit)
                throw new Exception(
                    $"Return: {exitCode}. Failed to run mmseqs {fullFilePath}; {processArgumentsString}.");
        }

        private void LogSomething(string s)
        {
            Console.WriteLine(s);
            _logger?.LogInformation(s);
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

    public class AutoMmseqsSettings
    {
        public string QdbSuffix { get; init; } = "_qdb";
        public string TempPath { get; set; } = Path.GetTempPath();
        public string Mmseqs2Internal_DbHeaderSuffix { get; init; } = "_h";
        public string Mmseqs2Internal_DbDataSuffix { get; init; } = String.Empty;
        public string Mmseqs2Internal_DbIndexSuffix { get; init; } = ".index";
        public string Mmseqs2Internal_DbLookupSuffix { get; init; }  = ".lookup";
        public string Mmseqs2Internal_DbTypeSuffix { get; init; } = ".dbtype";
        public string Mmseqs2Internal_ExpectedSeqDbSuffix => PreLoadDb ? ".idx" : "_seq";
        public string Mmseqs2Internal_ExpectedAlnDbSuffix => PreLoadDb ? ".idx" : "_aln";
        public string FastaSuffix { get; init; } = ".fasta";
        public Dictionary<string, string> Custom { get; init; } = new();
        public int ThreadCount { get; init; } = 1;
        public bool PreLoadDb { get; init; } = false;
        public int MaxDesiredBatchSize { get; init; } = 1600;
        public string MmseqsBinaryPath { get; set; }
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

        public void ApplyDefaults()
        {
            Parameters.ForEach(x=>x.Value=x.DefaultValue);
        }

        public List<ICommandLineParameter> Parameters { get; protected init; }

        public IEnumerable<ICommandLineParameter> GetNonDefault()
        {
            return Parameters.Where(x=> x.DefaultValue.CompareTo(x.Value) != 0);
        }

    }
}