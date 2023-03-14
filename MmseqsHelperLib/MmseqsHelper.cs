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
        private  string searchModule = Constants.ModuleStrings[SupportedMmseqsModule.Search];
        private  string moveModule = Constants.ModuleStrings[SupportedMmseqsModule.MoveDatabase];
        private  string linkModule = Constants.ModuleStrings[SupportedMmseqsModule.LinkDatabase];
        private  string alignModule = Constants.ModuleStrings[SupportedMmseqsModule.Align];
        private  string filterModule = Constants.ModuleStrings[SupportedMmseqsModule.FilterResult];
        private  string msaConvertModule = Constants.ModuleStrings[SupportedMmseqsModule.ConvertResultToMsa];
        private  string expandModule = Constants.ModuleStrings[SupportedMmseqsModule.ExpandAlignment];
        private string mergeModule = Constants.ModuleStrings[SupportedMmseqsModule.MergeDatabases];

        private readonly ILogger<MmseqsHelper> _logger;

        public MmseqsHelper(AutoMmseqsSettings? inputSettings, ILogger<MmseqsHelper> logger)
        {
            _logger = logger;
            Settings = inputSettings ?? GetDefaultSettings();

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


        }

        public AutoMmseqsSettings Settings { get; set; }

        public string MmseqsBinaryPath => Settings.MmseqsBinaryPath;


        public async Task AutoCreateColabfoldMonoDbsFromFastasAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludedIds, string outputPath)
        {
            var excludedIdList = excludedIds.ToList();
            var (existingTargets, missingTargets) = await GetExistingAndMissingSetsAsync(inputFastaPaths, existingDatabasePaths, excludedIdList);
            
            var batches = GetBatches(missingTargets);
                        
            foreach (var proteinBatch in batches)
            {
                await AutoProcessProteinBatch(outputPath, proteinBatch);
            }
        }

        private async Task AutoProcessProteinBatch(string outputPath, List<Protein> proteinBatch)
        {
            var batchId = Guid.NewGuid().ToString();

            var workingDir = Path.Join(Settings.TempPath, batchId);
            Directory.CreateDirectory(workingDir);

            LogSomething($"starting batch {batchId} with {proteinBatch.Count} items");

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
            var qdbNameBase = $"{Settings.QdbName}";
            var qdbPath = Path.Join(workingDir, qdbNameBase);

            var createDbParameters = new CreateDbParameters();
            createDbParameters.ApplyDefaults();
            await CreateDbAsync(queryFastaPath, qdbPath, createDbParameters);

            //*******************************************initial uniprot search to get search & profile dbs****************************
            var (searchDb, profileDb) = await Task.Run(() => AutoUniprotSearchAndCreateProfileAsync(workingDir, qdbPath));

            //*******************************************calc mono and pair dbs*******************************************************
            var uniprotMonoTask = Task.Run(() => AutoUniprotCreateMonoDbAsync(workingDir, qdbPath, searchDb, profileDb));
            var uniprotPairTask = Task.Run(() => AutoUniprotCreateAlignDbForPairAsync(workingDir, qdbPath, searchDb));
            var envDbMonoTask = Task.Run(() => AutoEnvDbCreateMonoDbAsync(workingDir, qdbPath, profileDb));

            await Task.WhenAll(uniprotMonoTask, uniprotPairTask, envDbMonoTask);


            //*******************************************merge the mono dbs*******************************************************
            var monoDbPath = uniprotMonoTask.Result;
            var pairDbPath = uniprotPairTask.Result;
            var envDbPath = envDbMonoTask.Result;

            var mergeMonoDbPath = await AutoMergeMonoDbsAsync(workingDir, qdbPath, monoDbPath, envDbPath);


            //*******************************************move the result  files to final output*************************************
            var finalPathQdb = Path.Join(outputPath, batchId, Settings.QdbName);
            var finalPathMonos = Path.Join(outputPath, batchId, Settings.MonoModeResultDbName);
            var finalPathPair = Path.Join(outputPath, batchId, Settings.PairModeFirstAlignDbName);

            var copyTasks = new List<Task>()
            {
                CopyDatabaseAsync(qdbPath, finalPathQdb),
                CopyDatabaseAsync(mergeMonoDbPath, finalPathMonos),
                CopyDatabaseAsync(pairDbPath, finalPathPair),
            };

            await Task.WhenAll(copyTasks);


            LogSomething(finalPathQdb);
            LogSomething(finalPathMonos);
            LogSomething(finalPathPair);
        }

        private async Task<(string searchDb, string profileDb)> AutoUniprotSearchAndCreateProfileAsync(string workingDir, string qdbPath)
        {
            //*******************************************search*******************************************************
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
            var movePosParams = new List<string>() { profileResultDbOriginal, profileResultDb };
            await RunMmseqsAsync(moveModule, movePosParams, String.Empty);

            //***link to header db of qdb since it has the same values***
            var linkPosParams = new List<string>() { qdbPath + Settings.Mmseqs2Internal_DbHeaderSuffix, profileResultDb + Settings.Mmseqs2Internal_DbHeaderSuffix };
            await RunMmseqsAsync(linkModule, linkPosParams, String.Empty);

            return (searchResultDb, profileResultDb);

        }

        private async Task<string> AutoMergeMonoDbsAsync(string workingDir, string qdb, string uniprotMonoDb, string envDbMonoDb)
        {
            var localProcessingPath = Path.Join(workingDir, "final");
            Directory.CreateDirectory(localProcessingPath);

            //*******************************************merge the mono dbs*******************************************************
            var mergeMonoResultDb = Path.Join(localProcessingPath, Settings.MonoModeResultDbName); 
            var mergePosParams = new List<string>()
            {
                qdb,
                mergeMonoResultDb,
                uniprotMonoDb,
                envDbMonoDb
            };
            await RunMmseqsAsync(mergeModule, mergePosParams, String.Empty);


            return mergeMonoResultDb;

        }

        private async Task CopyDatabaseAsync(string sourceDbPathWithDatabaseRootName, string targetDbPathWithDatabaseRootName)
        {
            var sourceDir = Path.GetDirectoryName(sourceDbPathWithDatabaseRootName) ?? String.Empty;
            var sourceDb = Path.GetFileName(sourceDbPathWithDatabaseRootName);

            var targetDir = Path.GetDirectoryName(targetDbPathWithDatabaseRootName) ?? String.Empty;
            var targetDb = Path.GetFileName(targetDbPathWithDatabaseRootName);

            await CopyDatabaseAsync(sourceDb, targetDb, sourceDir, targetDir);
        }

        private async Task<string> AutoEnvDbCreateMonoDbAsync(string workingDir, string qdbPath, string profileResultDb)
        {
            var localProcessingPath = Path.Join(workingDir, "env_mono");
            Directory.CreateDirectory(localProcessingPath);
            var targetDbPathBase = Settings.Custom["EnvDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
            var targetDbPathAln = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

            //*******************************************search*******************************************************
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

        private async Task<string> AutoUniprotCreateMonoDbAsync(string workingDir, string qdbPath, string searchResultDb, string profileResultDb)
        {
            var localProcessingPath = Path.Join(workingDir, "uniprot_mono");
            Directory.CreateDirectory(localProcessingPath);
            var targetDbPathBase = Settings.Custom["UniprotDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
            var targetDbPathAln = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

            //*******************************************expand*******************************************************
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

        private async Task<string> AutoUniprotCreateAlignDbForPairAsync(string workingDir, string qdbPath, string searchResultDb)
        {
            var localProcessingPath = Path.Join(workingDir, "uniprot_pair");
            Directory.CreateDirectory(localProcessingPath);
            var targetDbPathBase = Settings.Custom["UniprotDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
            var targetDbPathAln = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

            //*******************************************expand*******************************************************
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
            var alignResultDb = Path.Join(localProcessingPath, Settings.PairModeFirstAlignDbName);
            var alignPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                expandResultDb,
                alignResultDb,
                
            };
            await RunMmseqsAsync(alignModule, alignPosParams, $"{Settings.Custom["colabFold_AlignParamsPair"]} {Settings.Custom["performanceParams"]}");
            
            return alignResultDb;
        }

        private async Task CreateDbAsync(string queryFastaPath, string outputDbNameBase, CreateDbParameters createDbParameters)
        {
            await CreateDbAsync(new List<String> { queryFastaPath }, outputDbNameBase, createDbParameters);
        }

        private List<List<Protein>> GetBatches(List<Protein> proteins)
        {
            var batchCount = 1 + (proteins.Count - 1) / Settings.MaxDesiredBatchSize ;

            var batches = new List<List<Protein>>();

            //https://stackoverflow.com/a/4262134/4554766
            var rng = new Random();
            var counter = 0;
            var tempList = new List<Protein>();
            foreach (var protein in proteins.OrderBy(x=> rng.Next()))
            {
                tempList.Add(protein);
                counter++;
                if (counter == Settings.MaxDesiredBatchSize)
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
                if (x == null || y == null) return false;
                return x.Id.Equals(y.Id, StringComparison.InvariantCulture);
            }

            public override int GetHashCode(Protein obj)
            {
                return obj.GetHashCode();
            }
        }

        private async Task<(List<Protein> existing,List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludeIds)
        {
            var uniqueInputFastaEntries = new HashSet<Protein>(new ProteinByIdComparer());

            var excludedList = excludeIds.ToList();

            foreach (var inputFastaPath in inputFastaPaths)
            {
                var stream = File.OpenRead(inputFastaPath);
                var fastas = await FastaHelper.GetFastaEntriesIfValidAsync(stream, SequenceType.Protein);
                if (fastas is not null && fastas.Any())
                {
                    foreach (var fastaEntry in fastas)
                    {
                        var protein = new Protein()
                            { Id = Helper.GetMd5Hash(fastaEntry.Sequence), Sequence = fastaEntry.Sequence };
                        if (!excludedList.Contains(protein.Id))
                        {
                            uniqueInputFastaEntries.Add(protein);
                        }
                        
                    }
                }
            }

            var existing = new List<Protein>();

            var useExistingImplemented = false;
            if (useExistingImplemented)
            {
                foreach (var existingDatabasePath in existingDatabasePaths)
                {
                    var filesInThisPath = Directory.GetFiles(existingDatabasePath);

                    //TODO: not yet implemented

                    var qdbSets = new List<(string data, string dataIndex, string header, string headerIndex)>();
                    var indexFiles = filesInThisPath.Where(x =>
                        x.EndsWith($"{Settings.QdbName}${Settings.Mmseqs2Internal_DbHeaderSuffix}"));
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
#if DEBUG 
            return;
#endif

            var exitCode = await Helper.RunProcessAsync(fullFilePath, processArgumentsString);
            
            const int successExit = 0;
            if (exitCode != successExit)
                throw new Exception(
                    $"Return: {exitCode}. Failed to run mmseqs {fullFilePath}; {processArgumentsString}.");
        }

        private void LogSomething(string s)
        {
            // var timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            // Console.WriteLine($"[{timeStamp}]: {s}");
            _logger?.LogInformation(s);
        }

        private static string EnsureQuotedIfWhiteSpace(string input)
        {
            if (!HasWhitespace(input)) return input;
            return IsQuoted(input) ? input : $"\"{input}\"";
        }
        private static bool HasWhitespace(string input) => input.Any(x => Char.IsWhiteSpace(x));
        private static bool IsQuoted(string input) => input.Length > 2 && input.First() == '"' && input.Last() == '"';

        

        public async Task CopyDatabaseAsync(string sourceDbNameBase, string targetDbNameBase, string sourceFolder, string destinationFolder)
        {
            if (!IsValidDbName(sourceDbNameBase) || !IsValidDbName(targetDbNameBase))
            {
                throw new ArgumentException("Invalid Db name given");
            }

            if (sourceFolder.Any(x=> Path.GetInvalidPathChars().Contains(x))  || destinationFolder.Any(x => Path.GetInvalidPathChars().Contains(x)))
            {
                throw new ArgumentException("Invalid folder given");
            }

            if (!Directory.Exists(sourceFolder))
            {
                throw new ArgumentException("Source folder does not exist");
            }

            Directory.CreateDirectory(destinationFolder);

            var possibleExtensions = Settings.PossibleDbExtensions();
            var tasks = new List<Task>();

            foreach (var extension in possibleExtensions.Values)
            {
                var sourceFilename = $"{sourceDbNameBase}{extension}";
                var targetFilename = $"{targetDbNameBase}{extension}";
                tasks.Add(Helper.CopyFileIfExistsAsync(Path.Join(sourceFolder, sourceFilename), Path.Join(destinationFolder, targetFilename)));
            }

            await Task.WhenAll(tasks);

        }

        private bool IsValidDbName(string sourceDbNameBase)
        {
            return !sourceDbNameBase.Any(x => Path.GetInvalidFileNameChars().Contains(x) || char.IsWhiteSpace(x));
        }

        
    }
}