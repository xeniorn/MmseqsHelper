using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using AlphafoldPredictionLib;
using FastaHelperLib;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MmseqsHelperLib
{
    public class MmseqsHelper
    {
        private string searchModule = Constants.ModuleStrings[SupportedMmseqsModule.Search];
        private string moveModule = Constants.ModuleStrings[SupportedMmseqsModule.MoveDatabase];
        private string linkModule = Constants.ModuleStrings[SupportedMmseqsModule.LinkDatabase];
        private string alignModule = Constants.ModuleStrings[SupportedMmseqsModule.Align];
        private string filterModule = Constants.ModuleStrings[SupportedMmseqsModule.FilterResult];
        private string msaConvertModule = Constants.ModuleStrings[SupportedMmseqsModule.ConvertResultToMsa];
        private string expandModule = Constants.ModuleStrings[SupportedMmseqsModule.ExpandAlignment];
        private string mergeModule = Constants.ModuleStrings[SupportedMmseqsModule.MergeDatabases];

        private string pairModule = Constants.ModuleStrings[SupportedMmseqsModule.PairAlign];

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
                await AutoProcessProteinBatchIntoColabfoldMonoDbAsync(outputPath, proteinBatch);
            }
        }

        private async Task AutoProcessProteinBatchIntoColabfoldMonoDbAsync(string outputPath, List<Protein> proteinBatch)
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
            var qdbNameBase = $"{Settings.PersistedDbQdbName}";
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
            var finalPathQdb = Path.Join(outputPath, batchId, Settings.PersistedDbQdbName);
            var finalPathMonos = Path.Join(outputPath, batchId, Settings.PersistedDbMonoModeResultDbName);
            var finalPathPair = Path.Join(outputPath, batchId, Settings.PersistedDbPairModeFirstAlignDbName);

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
            var mergeMonoResultDb = Path.Join(localProcessingPath, Settings.PersistedDbMonoModeResultDbName);
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
            var alignResultDb = Path.Join(localProcessingPath, Settings.PersistedDbPairModeFirstAlignDbName);
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
            await CreateDbAsync(new List<string> { queryFastaPath }, outputDbNameBase, createDbParameters);
        }

        private List<List<T>> GetBatches<T>(List<T> sourceList, int desiredBatchSize)
        {
            var batchCount = 1 + (sourceList.Count - 1) / desiredBatchSize;

            var batches = new List<List<T>>();

            //https://stackoverflow.com/a/4262134/4554766
            var rng = new Random();
            var counter = 0;
            var tempList = new List<T>();
            foreach (var target in sourceList.OrderBy(x => rng.Next()))
            {
                tempList.Add(target);
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

        private List<List<PredictionTarget>> GetPredictionTargetBatches(List<PredictionTarget> predictionTargets)
        {
            return GetBatches<PredictionTarget>(predictionTargets,
                Settings.MaxDesiredPredictionTargetBatchSize);
        }

        private List<List<Protein>> GetBatches(List<Protein> proteins)
        {
            return GetBatches<Protein>(proteins,
                Settings.MaxDesiredBatchSize);
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

        private class PredictionTargetByIdComparer : EqualityComparer<PredictionTarget>
        {
            public override bool Equals(PredictionTarget? x, PredictionTarget? y)
            {
                if (x == null || y == null) return false;
                return x.UserProvidedId.Equals(y.UserProvidedId, StringComparison.InvariantCulture);
            }

            public override int GetHashCode(PredictionTarget obj)
            {
                return obj.GetHashCode();
            }
        }

        private async Task<(List<Protein> existing, List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludeIds)
        {
            var uniqueProteins = new HashSet<Protein>(new ProteinByIdComparer());

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
                            uniqueProteins.Add(protein);
                        }

                    }
                }
            }

            var (existing, missing) =
                await GetExistingAndMissingSetsAsync(uniqueProteins, existingDatabasePaths);

            return (existing, missing);

        }

        private async Task<(List<Protein> existing, List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<Protein> iproteins, IEnumerable<string> existingDatabasePaths)
        {
            var proteins = new HashSet<Protein>(iproteins, new ProteinByIdComparer());

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
                        x.EndsWith($"{Settings.PersistedDbQdbName}${Settings.Mmseqs2Internal_DbHeaderSuffix}"));
                }

            }

            var missing = proteins.Except(existing).ToList();

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

            if (sourceFolder.Any(x => Path.GetInvalidPathChars().Contains(x)) || destinationFolder.Any(x => Path.GetInvalidPathChars().Contains(x)))
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

        public async Task AutoCreateColabfoldA3msFromFastasGivenExistingMonoDbsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludedIds, string outputPath, IEnumerable<string> a3MPaths)
        {
            var excludedIdList = excludedIds.ToList();
            var inputPathsList = inputFastaPaths.ToList();
            var a3mPathsList = a3MPaths.ToList();
            
            var existingDatabaseFolderPaths = existingDatabasePaths.ToList();
            var dbEntryFolders = await GetDbEntryFoldersAsync(existingDatabaseFolderPaths);

            if (!dbEntryFolders.Any())
            {
                _logger.LogError("No valid folders found in provided source locations. Need pre-generated mono results to generate pair results.");
                return;
            }
            else
            {
                _logger.LogInformation($"Found {dbEntryFolders.Count} potential sources for mono predictions. Will proceed.");
            }
            
            var (existingTargets, missingTargets) =
                await GetExistingAndMissingPredictionTargetsAsync(inputPathsList, a3mPathsList, excludedIdList);

            if (!missingTargets.Any())
            {
                _logger.LogInformation("All targets already exist. No need to calculate any new MSAs.");
                return;
            }

            if (existingTargets.Any())
            {
                _logger.LogInformation($"{existingTargets.Count}/{existingTargets.Count + missingTargets.Count} targets already exist, will not recalculate those.");
            }

            var allMonos = missingTargets.SelectMany(x => x.UniqueProteins).DistinctBy(x => x.Sequence).ToList();

            var (existingMonos, missingMonos) = await GetExistingAndMissingSetsAsync(allMonos, dbEntryFolders);

            var batches = GetPredictionTargetBatches(missingTargets);

            foreach (var targetBatch in batches)
            {
                await AutoCreateColabfoldA3msFromFastasGivenExistingMonoDbsAsync(outputPath, dbEntryFolders, targetBatch);
            }
        }

        private async Task<List<string>> GetDbEntryFoldersAsync(List<string> existingDatabaseFolderPaths)
        {
            var result = new List<string>();

            foreach (var existingDatabaseFolderPath in existingDatabaseFolderPaths)
            {
                if (!Directory.Exists(existingDatabaseFolderPath))
                {
                    _logger.LogWarning($"Provided db path does not exist: {existingDatabaseFolderPath}");
                    continue;
                }

                var foldersInside = Directory.GetDirectories(existingDatabaseFolderPath);
                var validFolders = foldersInside.Where(x => IsValidDbFolder(x));
                result.AddRange(validFolders);
            }

            return result;
        }

        private bool IsValidDbFolder(string path)
        {
            if (!Directory.Exists(path)) return false;
            if (Directory.GetFiles(path).Length < Settings.PersistedDbMinimalNumberOfFilesInMonoDbResult) return false;
            return true;
        }

        private async Task AutoCreateColabfoldA3msFromFastasGivenExistingMonoDbsAsync(string outputPath, List<string> existingDatabasePaths, List<PredictionTarget> predictionBatch)
        {
            var batchId = Guid.NewGuid().ToString();

            var workingDir = Path.Join(Settings.TempPath, batchId);
            Directory.CreateDirectory(workingDir);

            LogSomething($"starting pairing batch {batchId} with {predictionBatch.Count} items");

            //TODO: check if it has all the required dbs: qdb header, (qdb seq => technically not really needed), aligndb, monoa3m
            // not sure where it's best to do this without duplicating the entire search. Probably step-wise, also to allow pair-only mode later

            //*******************************************construct the starting dbs from mono fragments****************************
            //*******************************************grab the relevant mono results*******************************************************
            var (pairedQdb, pairedAlignDb, unpairedA3mDb) = await AutoUniprotConstructPairQdbAndAlignDbAndUnpairA3mDbFromMonoDbsAsync(workingDir, existingDatabasePaths, predictionBatch);


            //*******************************************perform pairing*******************************************************
            var pairedDbPath = await AutoUniprotPerformPairingAsync(workingDir, pairedQdb, pairedAlignDb);
            

            //*******************************************move the result files to final output*************************************
            var finalPathQdb = Path.Join(outputPath, batchId, Settings.PersistedDbQdbName);
            var finalPathMonos = Path.Join(outputPath, batchId, Settings.PersistedDbMonoModeResultDbName);
            var finalPathPair = Path.Join(outputPath, batchId, Settings.PersistedDbPairModeFirstAlignDbName);

            //var copyTasks = new List<Task>()
            //{
            //    CopyDatabaseAsync(qdbPath, finalPathQdb),
            //    CopyDatabaseAsync(mergeMonoDbPath, finalPathMonos),
            //    CopyDatabaseAsync(pairDbPath, finalPathPair),
            //};

            //await Task.WhenAll(copyTasks);


            LogSomething(finalPathQdb);
            LogSomething(finalPathMonos);
            LogSomething(finalPathPair);
        }

        private async Task<string> AutoUniprotPerformPairingAsync(string workingDir, string qdbPath, string pairedAlignDb)
        {
            var targetDbPathBase = Settings.Custom["UniprotDbPath"];
            var targetDbPathSeq = targetDbPathBase + Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;

            var localProcessingPath = Path.Join(workingDir, "pairing");
            Directory.CreateDirectory(localProcessingPath);

            //*******************************************pair 1*******************************************************
            var pair1ResultDb = Path.Join(localProcessingPath, $"pair1");
            var pair1PosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                pairedAlignDb,
                pair1ResultDb,
            };
            await RunMmseqsAsync(pairModule, pair1PosParams, $"{Settings.Custom["performanceParams"]}");


            //*******************************************align*******************************************************
            var align2ResultDb = Path.Join(localProcessingPath, $"align2");
            var align2PosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                pair1ResultDb,
                align2ResultDb,
            };
            await RunMmseqsAsync(alignModule, align2PosParams, $"{Settings.Custom["colabFold_AlignParamsPair"]} {Settings.Custom["performanceParams"]}");

            //*******************************************pair 1*******************************************************
            var pair2ResultDb = Path.Join(localProcessingPath, $"pair2");
            var pair2PosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                align2ResultDb,
                pair2ResultDb,
            };
            await RunMmseqsAsync(pairModule, pair2PosParams, $"{Settings.Custom["performanceParams"]}");
            
            //*******************************************convert*******************************************************
            var msaConvertResultDb = Path.Join(localProcessingPath, $"uniref_pair.a3m");
            var msaConvertPosParams = new List<string>()
            {
                qdbPath,
                targetDbPathSeq,
                pair2ResultDb,
                msaConvertResultDb,
            };
            await RunMmseqsAsync(msaConvertModule, msaConvertPosParams, $"{Settings.Custom["colabFold_MsaConvertParamsPair"]} {Settings.Custom["performanceParams"]}");

            return msaConvertResultDb;
        }

        private async Task<(string pairedQdb, string pairedAlignDb, string unpairedA3mDb)> AutoUniprotConstructPairQdbAndAlignDbAndUnpairA3mDbFromMonoDbsAsync(
            string workingDir, List<string> existingDatabaseFolderPaths, List<PredictionTarget> predictionBatch)
        {
            Directory.CreateDirectory(workingDir);

            var pairedQdb = Path.Join(workingDir, "qdb");
            var pairedAlignDb = Path.Join(workingDir, "align1");
            var unpairedA3mDb = Path.Join(workingDir, "unpaired_a3m_mmseqsdb");

            //*******************************************figure out which mono dbs contain relevant entries at which indices*******************************************************
            //******************************************* and read in relevant fragments of align files*******************************************************

            var targetMonos = predictionBatch.SelectMany(x => x.UniqueProteins).DistinctBy(x => x.Sequence).ToList();

            // for each prediction target, its cognate monos
            var predToMonosDict = new Dictionary<PredictionTarget, List<Protein>>();

            // for each db, which monos it contains
            var dbToMonoMapping = new Dictionary<string, List<Protein>>();

            // for each mono, which db and which index within that db it has
            var monoToDbAndIndexMapping = new Dictionary<Protein, (string db, int index)>();

            // actual data of each aligndb for each mono
            var monoToAlignFragmentMappings = new Dictionary<Protein, byte[]>();

            foreach (var predictionTarget in predictionBatch)
            {
                var monos = predictionTarget.UniqueProteins.Select(x => targetMonos.Single(prot => prot.SameSequenceAs(x))).ToList();
                predToMonosDict.Add(predictionTarget, monos);
            }

            // (in parallel?) for each existing db file, check which of the target monos it contains
            var existingDbParallelSearchBatchSize = 20;
            var remainingMonos = new List<Protein>(targetMonos);

            var searchBatches = GetBatches<string>(existingDatabaseFolderPaths, existingDbParallelSearchBatchSize);
            foreach (var searchBatch in searchBatches)
            {
                var alreadyFound = monoToDbAndIndexMapping.Keys;
                remainingMonos = remainingMonos.Except(alreadyFound).ToList();

                await GetDbToMonoMappingsForSearchBatch(searchBatch, remainingMonos, dbToMonoMapping);
                await GetMonoToDbAndIndexMappingsForSearchBatch(searchBatch, dbToMonoMapping, monoToDbAndIndexMapping);
                await ReadInAlignDbFragmentsForSearchBatch(searchBatch, dbToMonoMapping, monoToDbAndIndexMapping, monoToAlignFragmentMappings);

                async Task GetDbToMonoMappingsForSearchBatch(List<string> dbLocationsToSearch, List<Protein> proteins,
                    Dictionary<string, List<Protein>> mutableDbToMonoMapping)
                {
                    var resultTasksMapping = new List<(string db, Task<List<string>> resultTask)>();
                    foreach (var dbLocation in dbLocationsToSearch)
                    {
                        // queue up tasks for now don't await one by one
                        var qdbPath = Path.Join(dbLocation, Settings.PersistedDbQdbName);
                        resultTasksMapping.Add((dbLocation, GetIdsFoundInSequenceDbAsync(qdbPath, proteins.Select(x => x.Id))));
                    }

                    // wait all in batch in parallel
                    await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

                    foreach (var (db, resultTask) in resultTasksMapping)
                    {
                        var ids = resultTask.Result;
                        var containedProteins = proteins.Where(x => ids.Contains(x.Id));
                        mutableDbToMonoMapping.Add(db, containedProteins.ToList());
                    }
                }

                async Task GetMonoToDbAndIndexMappingsForSearchBatch(List<string> dbLocationsToSearch,
                    Dictionary<string, List<Protein>> dbToMonoMapping, Dictionary<Protein, (string db, int index)> mutableMonoToDbAndIndexMapping)
                {
                    var resultTasksMapping = new List<(string db, Task<List<(string id, int index)>> resultTask)>();
                    foreach (var dbLocation in dbLocationsToSearch)
                    {
                        var proteinsInThisDb = dbToMonoMapping[dbLocation];

                        // queue up tasks for now don't await one by one
                        var qdbPath = Path.Join(dbLocation, Settings.PersistedDbQdbName);
                        resultTasksMapping.Add((dbLocation, GetIdsAndIndicesFoundInSequenceDbAsync(qdbPath, proteinsInThisDb.Select(x => x.Id).ToList())));
                    }

                    await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

                    foreach (var (dbPath, resultTask) in resultTasksMapping)
                    {
                        var proteinsInThisDb = dbToMonoMapping[dbPath];
                        var entriesInDb = resultTask.Result;
                        foreach (var (id, index) in entriesInDb)
                        {
                            var protein = proteinsInThisDb.Single(x => x.Id == id);
                            mutableMonoToDbAndIndexMapping.Add(protein, (dbPath, index));
                        }
                    }
                }

                async Task ReadInAlignDbFragmentsForSearchBatch(List<string> dbLocationsToProcess,
                    Dictionary<string, List<Protein>> dbToMonoMapping, Dictionary<Protein, (string db, int index)> monoToDbAndIndexMapping,
                    Dictionary<Protein, byte[]> mutableMonoToAlignFragmentMapping)
                {
                    var resultTasksMapping = new List<(string db, Task<List<(byte[] data, int index)>> resultTask)>();
                    foreach (var dbLocation in dbLocationsToProcess)
                    {
                        var proteinsInThisDb = dbToMonoMapping[dbLocation];
                        var indices = monoToDbAndIndexMapping.Where(x => x.Value.db == dbLocation && proteinsInThisDb.Contains(x.Key))
                            .Select(x => x.Value.index).ToList();

                        // queue up tasks for now don't await one by one
                        var alignDb = Path.Join(dbLocation, Settings.PersistedDbPairModeFirstAlignDbName);
                        resultTasksMapping.Add((dbLocation, ReadEntriesWithIndicesFromAlignDbAsync(alignDb, indices)));
                    }

                    await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

                    foreach (var (dbPath, resultTask) in resultTasksMapping)
                    {
                        var proteinsInThisDb = dbToMonoMapping[dbPath];
                        var indexedData = resultTask.Result;
                        foreach (var (data, index) in indexedData)
                        {
                            var protein = proteinsInThisDb.Single(x => monoToDbAndIndexMapping[x].db == dbPath && monoToDbAndIndexMapping[x].index == index);
                            mutableMonoToAlignFragmentMapping.Add(protein, data);
                        }
                    }
                }
            }

            //*******************************************construct pair dbs from known sources*******************************************************

            var generatedMonoIndex = 0;
            var generatedPredictionIndex = 0;

            var alignDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.Alignment_ALIGNMENT_RES);
            var qdbDataDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.Sequence_AMINO_ACIDS);
            var qdbHeaderDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.Header_GENERIC_DB);
            var unpairedA3mDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.A3m_MSA_DB);

            var qdbLookupFileFragments = new List<MmseqsLookupEntry>();

            var monoToNewIdMapping = new Dictionary<Protein, int>();
            var predictionToNewPairIdMapping = new Dictionary<PredictionTarget, int>();

            foreach (var predictionTarget in predictionBatch)
            {
                foreach (var targetProtein in predictionTarget.UniqueProteins)
                {
                    // take the protein from the prefiltered references, avoiding multiple references to equivalent entity
                    var protein = targetMonos.Single(x => x.Id == targetProtein.Id);
                    var data = monoToAlignFragmentMappings[protein];
                    
                    qdbDataDbObject.Add(Encoding.ASCII.GetBytes(protein.Sequence), generatedMonoIndex);
                    qdbHeaderDbObject.Add(Encoding.ASCII.GetBytes(protein.Id), generatedMonoIndex);
                    alignDbObject.Add(data, generatedMonoIndex);
                    qdbLookupFileFragments.Add(new MmseqsLookupEntry(generatedMonoIndex, protein.Id, generatedPredictionIndex));
                    
                    monoToNewIdMapping.Add(protein, generatedMonoIndex);
                    generatedMonoIndex++;
                }

                predictionToNewPairIdMapping.Add(predictionTarget, generatedPredictionIndex);
                generatedPredictionIndex++;
            }
            
            var pairQdbDataDbPath = $"{pairedQdb}{Settings.Mmseqs2Internal_DbDataSuffix}";
            var pairQdbHeaderDbPath = $"{pairedQdb}{Settings.Mmseqs2Internal_DbHeaderSuffix}";
            var alignDbDataDbPath = $"{pairedAlignDb}{Settings.Mmseqs2Internal_DbDataSuffix}";
            var unpairedA3mDbDataDbPath = $"{unpairedA3mDb}{Settings.Mmseqs2Internal_DbDataSuffix}";

            var writeTasks = new List<Task>
            {
                qdbDataDbObject.WriteToFileSystemAsync(Settings, pairQdbDataDbPath),
                qdbHeaderDbObject.WriteToFileSystemAsync(Settings, pairQdbHeaderDbPath),
                alignDbObject.WriteToFileSystemAsync(Settings, alignDbDataDbPath),
                unpairedA3mDbObject.WriteToFileSystemAsync(Settings, unpairedA3mDbDataDbPath)
            };

            await Task.WhenAll(writeTasks);
            
            return (pairedQdb, pairedAlignDb, unpairedA3mDb);

        }

        private async Task<List<(byte[] data, int index)>> ReadEntriesWithIndicesFromAlignDbAsync(string alignDbPath, List<int> indices)
        {
            var result = new List<(byte[] data, int index)>();

            var alignDbIndexFile = $"{alignDbPath}{Settings.Mmseqs2Internal_DbIndexSuffix}";
            var entries = await GetAllIndexFileEntriesInDbAsync(alignDbIndexFile);
            var orderedEntriesToRead = entries.Where(x => indices.Contains(x.index)).OrderBy(x => x.startOffset).ToList();

            var alignDbDataFile = $"{alignDbPath}{Settings.Mmseqs2Internal_DbDataSuffix}";

            using (BinaryReader reader = new BinaryReader(new FileStream(alignDbDataFile, FileMode.Open)))
            {
                foreach (var (index, startOffset, length) in orderedEntriesToRead)
                {
                    reader.BaseStream.Position = startOffset;
                    var lengthToRead = length - Settings.Mmseqs2Internal_DataEntrySeparator.Length;
                    var data = reader.ReadBytes(lengthToRead);
                    result.Add((data, index));
                }
            }

            return result;

        }

        private async Task<List<(string id, int index)>> GetIdsAndIndicesFoundInSequenceDbAsync(string sequenceDbPath, List<string> idsToSearch)
        {
            var headerFile = $"{sequenceDbPath}{Settings.Mmseqs2Internal_DbHeaderSuffix}";
            var headersInFile = await GetAllHeadersInSequenceDbHeaderDbAsync(headerFile);
            var headerToStartAndLengthMappings = new Dictionary<string, (int startOffset, int length)>();

            var startOffset = 0;
            foreach (var header in headersInFile)
            {
                var len = header.Length + Settings.Mmseqs2Internal_DataEntrySeparator.Length;
                // all must be enumerated to get the correct offsets, but only the selected ones get added to the list
                if (idsToSearch.Contains(header))
                {
                    headerToStartAndLengthMappings.Add(header, (startOffset, len));
                }
                startOffset += len;
            }

            var headerIndexFile = $"{sequenceDbPath}{Settings.Mmseqs2Internal_DbHeaderSuffix}{Settings.Mmseqs2Internal_DbIndexSuffix}";
            var headerIndexFileEntries = await GetAllIndexFileEntriesInDbAsync(headerIndexFile);

            var result = new List<(string id, int index)>();

            foreach (var (header, (offset, length)) in headerToStartAndLengthMappings)
            {
                var matchedEntryIndex = headerIndexFileEntries.FindIndex(x => x.startOffset == offset && x.length == length);
                var foundMatch = matchedEntryIndex >= 0;
                if (foundMatch)
                {
                    result.Add((header, headerIndexFileEntries[matchedEntryIndex].index));
                }
            }

            return result;
        }

        private async Task<List<(int index, int startOffset, int length)>> GetAllIndexFileEntriesInDbAsync(string mmseqsDbIndexFile)
        {
            var allLines = await File.ReadAllLinesAsync(mmseqsDbIndexFile);
            return allLines.Select(x =>
            {
                var entries = x.Split(Settings.Mmseqs2Internal_IndexColumnSeparator);
                return (int.Parse(entries[0]), int.Parse(entries[1]), int.Parse(entries[2]));
            }).ToList();
        }

        private async Task<List<string>> GetIdsFoundInSequenceDbAsync(string dbPath, IEnumerable<string> idsToSearch)
        {
            var headerFile = $"{dbPath}{Settings.Mmseqs2Internal_DbHeaderSuffix}";
            var headersInFile = await GetAllHeadersInSequenceDbHeaderDbAsync(headerFile);
            return idsToSearch.Where(x => headersInFile.Contains(x)).ToList();

        }

        private async Task<List<string>> GetAllHeadersInSequenceDbHeaderDbAsync(string headerDbFile)
        {
            var allText = await File.ReadAllTextAsync(headerDbFile);
            // each line is terminated with newline and ascii null character
            var lines = allText.Split(Settings.Mmseqs2Internal_DataEntrySeparator);
            // last line in collection is empty since even the last line is terminated with newline and null char, remove
            var headers = lines.Take(lines.Length - 1).ToList();
            return headers;
        }

        private async Task<(List<PredictionTarget> existing, List<PredictionTarget> missing)> GetExistingAndMissingPredictionTargetsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludeIds)
        {
            var uniqueInputFastaEntries = new HashSet<PredictionTarget>(new PredictionTargetByIdComparer());

            var excludedList = excludeIds.ToList();

            var targets = new List<PredictionTarget>();

            var importer = new Importer();

            foreach (var inputFastaPath in inputFastaPaths)
            {
                var stream = File.OpenRead(inputFastaPath);
                var fastas = await FastaHelper.GetFastaEntriesIfValidAsync(stream, SequenceType.Protein, keepCharacters:":");
                if (fastas is not null && fastas.Any())
                {
                    var newPredictions =
                        await importer.ImportPredictionTargetsFromComplexProteinFastaFileAllowingMultimersAsync(fastas);
                    // auto-id proteins with their hash
                    newPredictions.ForEach(x=>x.UniqueProteins.ForEach(prot=>prot.Id = Helper.GetMd5Hash(prot.Sequence)));
                    targets.AddRange(newPredictions);
                }
            }

            var finalPredictions = targets.Where(x => !excludedList.Contains(x.AutoIdFromConstituents))
                .DistinctBy(x => x.AutoIdFromConstituents).ToList();

            return await GetExistingAndMissingPredictionTargetsAsync(finalPredictions, existingDatabasePaths);



        }

        private async Task<(List<PredictionTarget> existing, List<PredictionTarget> missing)> GetExistingAndMissingPredictionTargetsAsync(List<PredictionTarget> filteredUniquePredictions, IEnumerable<string> existingDatabasePaths)
        {

            var existing = new List<PredictionTarget>();

            var useExistingImplemented = false;
            if (useExistingImplemented)
            {
                foreach (var existingDatabasePath in existingDatabasePaths)
                {
                    var filesInThisPath = Directory.GetFiles(existingDatabasePath);

                    //TODO: not yet implemented

                    var qdbSets = new List<(string data, string dataIndex, string header, string headerIndex)>();
                    var indexFiles = filesInThisPath.Where(x =>
                        x.EndsWith($"{Settings.PersistedDbQdbName}${Settings.Mmseqs2Internal_DbHeaderSuffix}"));
                }

            }

            var missing = filteredUniquePredictions.Except(existing).ToList();

            return (existing, missing);
        }
    }
}