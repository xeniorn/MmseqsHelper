using System.Security.Cryptography.X509Certificates;
using System.Text;
using AlphafoldPredictionLib;
using FastaHelperLib;
using Microsoft.Extensions.Logging;

namespace MmseqsHelperLib;

public class ColabfoldMmseqsHelper
{

    private readonly ILogger<ColabfoldMmseqsHelper> _logger;

    public ColabfoldMmseqsHelper(AutoColabfoldMmseqsSettings? inputSettings, ILogger<ColabfoldMmseqsHelper> logger, MmseqsHelper mmseqsHelper)
    {
        _logger = logger;
        Settings = inputSettings ?? GetDefaultSettings();
        Mmseqs = mmseqsHelper;

        //TODO: make all this proper config subobjects, not "custom" like this. Settings.Colabfold.SearchParamsShared, etc

        //#region HardcodedSetup
        //const string colabFold_SearchParamsShared = @"--num-iterations 3 -a -s 8 -e 0.1 --max-seqs 10000";
        //Settings.Custom.Add("colabFold_SearchParamsShared", colabFold_SearchParamsShared);
        
        //const string colabFold_ExpandParamsUnirefMono =
        //    @"--expansion-mode 0 -e inf --expand-filter-clusters 1 --max-seq-id 0.95";
        //Settings.Custom.Add("colabFold_ExpandParamsUnirefMono", colabFold_ExpandParamsUnirefMono);
        //const string colabFold_ExpandParamsUnirefPair =
        //    @"--expansion-mode 0 -e inf --expand-filter-clusters 0 --max-seq-id 0.95";
        //Settings.Custom.Add("colabFold_ExpandParamsUnirefPair", colabFold_ExpandParamsUnirefPair);
        //const string colabFold_ExpandParamsEnvMono =
        //    @"--expansion-mode 0 -e inf";
        //Settings.Custom.Add("colabFold_ExpandParamsEnvMono", colabFold_ExpandParamsEnvMono);

        //const string colabFold_FilterParams =
        //    @"--qid 0 --qsc 0.8 --diff 0 --max-seq-id 1.0 --filter-min-enable 100";
        //Settings.Custom.Add("colabFold_FilterParams", colabFold_FilterParams);

        //const string colabFold_AlignParamsMono = @"-e 10  --max-accept 1000000 --alt-ali 10 -a";
        //Settings.Custom.Add("colabFold_AlignParamsMono", colabFold_AlignParamsMono);
        //const string colabFold_Align1ParamsPair = @"-e 0.001  --max-accept 1000000 -c 0.5 --cov-mode 1";
        //Settings.Custom.Add("colabFold_Align1ParamsPair", colabFold_Align1ParamsPair);
        //const string colabFold_Align2ParamsPair = @"-e inf";
        //Settings.Custom.Add("colabFold_Align2ParamsPair", colabFold_Align2ParamsPair);

        //const string colabFold_MsaConvertParamsMono = @"--msa-format-mode 6 --filter-msa 1 --filter-min-enable 1000 --diff 3000 --qid '0.0,0.2,0.4,0.6,0.8,1.0' --qsc 0 --max-seq-id 0.95";
        //Settings.Custom.Add("colabFold_MsaConvertParamsMono", colabFold_MsaConvertParamsMono);
        //const string colabFold_MsaConvertParamsPair = @"--msa-format-mode 5";
        //Settings.Custom.Add("colabFold_MsaConvertParamsPair", colabFold_MsaConvertParamsPair);
        
        //#endregion


    }

    public MmseqsHelper Mmseqs { get; }
    public AutoColabfoldMmseqsSettings Settings { get; set; }

    public static AutoColabfoldMmseqsSettings GetDefaultSettings() => new AutoColabfoldMmseqsSettings();

    public async Task GenerateA3msFromFastasGivenExistingMonoDbsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludedIds, string outputPath, IEnumerable<string> a3mPaths)
    {
        var excludedIdList = excludedIds.ToList();
        var inputPathsList = inputFastaPaths.ToList();
        var a3mPathsList = a3mPaths.ToList();

        var existingDatabaseFolderPaths = existingDatabasePaths.ToList();
        var dbEntryFolders = await GetDbEntryFoldersAsync(existingDatabaseFolderPaths);

        if (!dbEntryFolders.Any())
        {
            _logger.LogError("No valid folders found in provided source locations. Need pre-generated mono results to generate pair results.");
            return;
        }
        
        _logger.LogInformation($"Found {dbEntryFolders.Count} potential sources for mono predictions. Will proceed.");
        
        // rectify all targets, giving them standard ordering, capitalization, and referencing the same set of Protein instances
        var rectifiedPredictionTargets = await GetRectifiedTargetPredictionsAsync(inputPathsList, excludedIdList);

        var (existingTargets, missingTargets) =
            await GetExistingAndMissingPredictionTargetsAsync(rectifiedPredictionTargets, a3mPathsList);

        if (!missingTargets.Any())
        {
            _logger.LogInformation("All targets already exist. No need to calculate any new MSAs.");
            return;
        }
        if (existingTargets.Any())
        {
            _logger.LogInformation($"{existingTargets.Count}/{existingTargets.Count + missingTargets.Count} targets already exist, will not recalculate those.");
        }

        // all predictions use same mono references (are "rectified"), distinct by reference is ok
        var allMonos = missingTargets.SelectMany(x => x.UniqueProteins).Distinct().ToList();
        var (existingMonos, missingMonos) = await GetExistingAndMissingSetsAsync(allMonos, dbEntryFolders);

        var targetsMissingMonos = missingTargets
            .Where(x => x.UniqueProteins.Any(pr => missingMonos.Contains(pr))).ToList();
        var predictableTargets = missingTargets.Except(targetsMissingMonos).ToList();

        if (targetsMissingMonos.Any())
        {
            _logger.LogWarning($"Some of the provided targets don't have monos required for MSA assembly {targetsMissingMonos.Count}/{missingTargets.Count})");
        }
        if (!predictableTargets.Any())
        {
            _logger.LogWarning("No further targets can be predicted due to missing mono predictions in the provided locations.");
            return;
        }

        _logger.LogInformation($"Will generate pair results for {predictableTargets.Count} targets.");
        
        var batches = GetPredictionTargetBatches(predictableTargets);
        foreach (var targetBatch in batches)
        {
            await GenerateA3msFromFastasGivenExistingMonoDbsAsync(outputPath, dbEntryFolders, targetBatch);
        }
    }
    /// <summary>
    /// Will load up targets from the source fasta files, make it so that same prot sequences refer to same Protein instances, and remove any duplicate target.
    /// Within each target, the ordering will be clearly defined based on constituents, sorted first by sequence length then lexically
    /// </summary>
    /// <param name="inputPathsList"></param>
    /// <param name="excludedIds"></param>
    /// <returns></returns>
    private async Task<List<PredictionTarget>> GetRectifiedTargetPredictionsAsync(List<string> inputPathsList, List<string> excludedIds)
    {
        var monos = new HashSet<Protein>();

        var excludedList = excludedIds.ToList();

        var rectifiedTargets = new List<(string hash, PredictionTarget target)>();
        var duplicateTargets = new List<PredictionTarget>();
        var skippedTargets = new List<PredictionTarget>();

        var importer = new Importer();

        foreach (var inputFastaPath in inputPathsList)
        {
            var stream = File.OpenRead(inputFastaPath);
            var fastaEntries = await FastaHelper.GetFastaEntriesIfValidAsync(stream, SequenceType.Protein, keepCharacters: Settings.ColabfoldComplexFastaMonomerSeparator);
            if (fastaEntries is not null)
            {
                // TODO: this is all dirty and should be refactored at some point. One shouldn't hack up individual elements of Prediction Target like that, that part should be private
                foreach (var fastaEntry in fastaEntries)
                {
                    var prediction =
                        await importer.GetPredictionTargetFromComplexProteinFastaEntryAllowingMultimersAsync(fastaEntry, Settings.ColabfoldComplexFastaMonomerSeparator);
                    var rectifiedPrediction = Helper.GetStandardSortedPredictionTarget(prediction);
                    
                    for (int i = 0; i < rectifiedPrediction.UniqueProteins.Count; i++)
                    {
                        var protein = rectifiedPrediction.UniqueProteins[i];
                        var existingMono = monos.SingleOrDefault(x => x.Id == protein.Id);
                        if (existingMono is not null)
                        {
                            rectifiedPrediction.UniqueProteins.RemoveAt(i);
                            rectifiedPrediction.UniqueProteins.Insert(i, existingMono);
                        }
                        else
                        {
                            monos.Add(protein);
                            rectifiedPrediction.UniqueProteins.Add(protein);
                        }
                    }

                    var predictionHash = Helper.GetAutoHashIdWithoutMultiplicity(rectifiedPrediction);

                    var index = rectifiedTargets.FindIndex(x => x.hash.Equals(predictionHash));
                    var found = index >= 0;
                    if (found)
                    {
                        duplicateTargets.Add(rectifiedPrediction);
                    }
                    else
                    {
                        if (!excludedList.Contains(predictionHash))
                        {
                            rectifiedTargets.Add((predictionHash, rectifiedPrediction));
                        }
                        else
                        {
                            skippedTargets.Add(rectifiedPrediction);
                        }
                    }
                }
            }
        }

        if (duplicateTargets.Any())
        {
            _logger.LogInformation($"Some inputs ({duplicateTargets.Count}) map to identical predictions, those will be skipped.");
        }

        if (duplicateTargets.Any())
        {
            _logger.LogInformation($"Some inputs ({skippedTargets.Count}) were excluded based on the provided exclusion id list.");
        }

        var targets = rectifiedTargets.Select(x => x.target).ToList();
        
        return targets;
    }

    public async Task AutoCreateColabfoldMonoDbsFromFastasAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabasePaths, IEnumerable<string> excludedIds, string outputPath)
    {
        var excludedIdList = excludedIds.ToList();
        var (existingTargets, missingTargets) = await GetExistingAndMissingSetsAsync(inputFastaPaths, existingDatabasePaths, excludedIdList);

        var total = existingTargets.Count + missingTargets.Count;

        if (existingTargets.Any())
        {
            _logger.LogInformation($"Some monos ({existingTargets.Count}/{total}) have existing results, will skip.");
        }

        if (!missingTargets.Any())
        {
            _logger.LogInformation("All input monos already have existing results.");
            return;
        }

        _logger.LogInformation($"Running predictions for {missingTargets.Count} monos.");

        var batches = GetBatches(missingTargets);

        foreach (var proteinBatch in batches)
        {
            await AutoProcessProteinBatchIntoColabfoldMonoDbAsync(outputPath, proteinBatch);
        }
    }

    private async Task GenerateA3msFromFastasGivenExistingMonoDbsAsync(string outputPath, List<string> existingDatabasePaths, List<PredictionTarget> predictionBatch)
    {
        var batchId = Guid.NewGuid().ToString();

        var workingDir = Path.Join(Settings.TempPath, batchId);
        Directory.CreateDirectory(workingDir);

        LogSomething($"starting pairing batch {batchId} with {predictionBatch.Count} items");

        //TODO: check if it has all the required dbs: qdb header, (qdb seq => technically not really needed), aligndb, monoa3m
        // not sure where it's best to do this without duplicating the entire search. Probably step-wise, also to allow pair-only mode later

        //*******************************************construct the starting dbs from mono fragments****************************
        //*******************************************grab the relevant mono results*******************************************************
        var predictionToIndexMapping = new Dictionary<PredictionTarget, List<int>>();
        LogSomething($"Collecting mono data required for pairing...");
        var (pairedQdb, pairedAlignDb, unpairedA3mDb) = await AutoUniprotConstructPairQdbAndAlignDbAndUnpairA3mDbFromMonoDbsAsync(workingDir, existingDatabasePaths, predictionBatch, predictionToIndexMapping);

        //*******************************************perform pairing*******************************************************
        LogSomething($"Performing MSA pairing...");
        var pairedDbPath = await AutoUniprotPerformPairingAsync(workingDir, pairedQdb, pairedAlignDb);

        //*******************************************construct invdividual result dbs*******************************************************
        LogSomething($"Combining paired and unpaired data...");
        var colabfoldMsaObjects =
            await AutoCreateColabfoldMsaObjectsAsync(predictionBatch, unpairedA3mDb, pairedDbPath, predictionToIndexMapping);

        //*******************************************write the result files*************************************
        LogSomething($"Writing {colabfoldMsaObjects.Count} result files in {outputPath}...");
        foreach (var msaObject in colabfoldMsaObjects)
        {
            var autoName = msaObject.HashId;
            var subfolder = autoName.Substring(0, Settings.PairResultDatabaseSubfolderLength);
            var targetFolder = Path.Combine(outputPath, subfolder);
            Directory.CreateDirectory(targetFolder);
            var fullFilePath = $"{Path.Join(targetFolder, autoName)}{Settings.PersistedDbFinalA3mExtension}";
            await File.WriteAllBytesAsync(fullFilePath, msaObject.GetBytes());
        }

    }

    private async Task<List<ColabFoldMsaObject>> AutoCreateColabfoldMsaObjectsAsync(List<PredictionTarget> predictions, string unpairedA3MDb, string pairedDbPath, Dictionary<PredictionTarget, List<int>> predictionToIndexMapping)
    {
        var result = new List<ColabFoldMsaObject>();

        foreach (var predictionTarget in predictions)
        {
            var indices = predictionToIndexMapping[predictionTarget];

            var unpairedDataCollection = await Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(unpairedA3MDb, indices);
            var pairedDataCollection = await Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(pairedDbPath, indices);

            var unpairedDataDict = new Dictionary<Protein, byte[]>();
            var pairedDataDict = new Dictionary<Protein, byte[]>();

            for (int i = 0; i < predictionTarget.UniqueProteins.Count; i++)
            {
                var protein = predictionTarget.UniqueProteins[i];

                var unpairedData = unpairedDataCollection.Single(x => x.index == indices[i]).data;
                unpairedDataDict.Add(protein, unpairedData);
                var pairedData = pairedDataCollection.Single(x => x.index == indices[i]).data;
                pairedDataDict.Add(protein, pairedData);
            }

            var msaObj = new ColabFoldMsaObject(pairedDataDict, unpairedDataDict, predictionTarget);
            result.Add(msaObj);
        }

        return result;
    }

    private async Task<string> AutoEnvDbCreateMonoDbAsync(string workingDir, string qdbPath, string profileResultDb)
    {
        var localProcessingPath = Path.Join(workingDir, "env_mono");
        Directory.CreateDirectory(localProcessingPath);
        var targetDbPathBase = Settings.Custom["EnvDbPath"];
        var targetDbPathSeq = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
        var targetDbPathAln = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.searchModule, searchPosParams, $"{Settings.Custom["colabFold_SearchParamsShared"]} {Mmseqs.PerformanceParams}");

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.Custom["colabFold_ExpandParamsEnvMono"]} {Mmseqs.PerformanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, $"align");
        var alignPosParams = new List<string>()
        {
            profileResultDb,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.Custom["colabFold_AlignParamsMono"]} {Mmseqs.PerformanceParams}");

        //*******************************************filter*******************************************************
        var filterResultDb = Path.Join(localProcessingPath, $"filter");
        var filterPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            alignResultDb,
            filterResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.filterModule, filterPosParams, $"{Settings.Custom["colabFold_FilterParams"]} {Mmseqs.PerformanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"env_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            filterResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.Custom["colabFold_MsaConvertParamsMono"]} {Mmseqs.PerformanceParams}");

        return msaConvertResultDb;
    }

    private async Task<string> AutoMergeMonoDbsAsync(string workingDir, string qdb, string uniprotMonoDb, string envDbMonoDb)
    {
        var localProcessingPath = Path.Join(workingDir, "final");
        Directory.CreateDirectory(localProcessingPath);

        //*******************************************merge the mono dbs*******************************************************
        var mergeMonoResultDb = Path.Join(localProcessingPath, Settings.PersistedDbMonoModeResultDbName);
        var (success, resultDbPath) = await Mmseqs.RunMergeAsync(qdb, mergeMonoResultDb, new List<string> { uniprotMonoDb, envDbMonoDb }, String.Empty);

        if (!success)
        {
            _logger.LogError("Merge failed for some reason.");
            throw new Exception();
        }

        return resultDbPath;

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
        await Mmseqs.CreateDbAsync(queryFastaPath, qdbPath, createDbParameters);

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
            Mmseqs.CopyDatabaseAsync(qdbPath, finalPathQdb),
            Mmseqs.CopyDatabaseAsync(mergeMonoDbPath, finalPathMonos),
            Mmseqs.CopyDatabaseAsync(pairDbPath, finalPathPair),
        };

        await Task.WhenAll(copyTasks);


        LogSomething(finalPathQdb);
        LogSomething(finalPathMonos);
        LogSomething(finalPathPair);
    }

    private async Task<(string pairedQdb, string pairedAlignDb, string unpairedA3mDb)> AutoUniprotConstructPairQdbAndAlignDbAndUnpairA3mDbFromMonoDbsAsync(
        string workingDir, List<string> existingDatabaseFolderPaths, List<PredictionTarget> predictionBatch, Dictionary<PredictionTarget, List<int>> mutablePredictionToIndexMapping)
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
        var monoToDbAndIndexMappingMulti = new Dictionary<Protein, List<(string db, int index)>>();
        var monoToDbAndIndexMapping = new Dictionary<Protein, (string db, int index)>();

        // actual data of each aligndb for each mono
        var monoToAlignFragmentMappings = new Dictionary<Protein, byte[]>();

        // actual unpaired data
        var monoToUnpairedA3mMappings = new Dictionary<Protein, byte[]>();

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

            // preconstruct all objects
            {
                await GetDbToMonoMappingsForSearchBatch(searchBatch, remainingMonos, dbToMonoMapping);
                await GetMonoToDbAndIndexMappingsForSearchBatch(searchBatch, dbToMonoMapping,
                    monoToDbAndIndexMappingMulti);
                foreach (var (protein, dbIndexLocations) in monoToDbAndIndexMappingMulti)
                {
                    var (db, index) = dbIndexLocations.First();
                    if (dbIndexLocations.Count > 1)
                    {
                        _logger.LogInformation(
                            $"target {protein.Id} found in multiple mono databases ({dbIndexLocations.Count}), will use only the first one ({db})");
                    }

                    monoToDbAndIndexMapping.Add(protein, (db, index));
                }

                await ReadInAlignDbFragmentsForSearchBatch(searchBatch, dbToMonoMapping, monoToDbAndIndexMapping,
                    monoToAlignFragmentMappings);
                await ReadInUnpairedA3mDbFragmentsForSearchBatch(searchBatch, dbToMonoMapping,
                    monoToDbAndIndexMapping,
                    monoToUnpairedA3mMappings);
            }


            async Task GetDbToMonoMappingsForSearchBatch(List<string> dbLocationsToSearch, List<Protein> proteins,
                Dictionary<string, List<Protein>> mutableDbToMonoMapping)
            {
                var resultTasksMapping = new List<(string db, Task<List<string>> resultTask)>();
                foreach (var dbLocation in dbLocationsToSearch)
                {
                    // queue up tasks for now don't await one by one
                    var qdbPath = Path.Join(dbLocation, Settings.PersistedDbQdbName);
                    resultTasksMapping.Add((dbLocation, Mmseqs.GetIdsFoundInSequenceDbAsync(qdbPath, proteins.Select(x => x.Id))));
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
                Dictionary<string, List<Protein>> dbToMonoMapping, Dictionary<Protein, List<(string db, int index)>> mutableMonoToDbAndIndexMapping)
            {
                var resultTasksMapping = new List<(string db, Task<List<(string id, int index)>> resultTask)>();
                foreach (var dbLocation in dbLocationsToSearch)
                {
                    var proteinsInThisDb = dbToMonoMapping[dbLocation];

                    // queue up tasks for now don't await one by one
                    var qdbPath = Path.Join(dbLocation, Settings.PersistedDbQdbName);
                    resultTasksMapping.Add((dbLocation, Mmseqs.GetHeaderAndIndexForGivenIdsInSequenceDbAsync(qdbPath, proteinsInThisDb.Select(x => x.Id).ToList())));
                }

                await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

                foreach (var (dbPath, resultTask) in resultTasksMapping)
                {
                    var proteinsInThisDb = dbToMonoMapping[dbPath];
                    var entriesInDb = resultTask.Result;
                    foreach (var (id, index) in entriesInDb)
                    {
                        var protein = proteinsInThisDb.Single(x => x.Id == id);
                        if (!mutableMonoToDbAndIndexMapping.ContainsKey(protein))
                        {
                            mutableMonoToDbAndIndexMapping.Add(protein, new List<(string db, int index)>());
                        }
                        mutableMonoToDbAndIndexMapping[protein].Add((dbPath, index));
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
                    resultTasksMapping.Add((dbLocation, Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(alignDb, indices)));
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

            async Task ReadInUnpairedA3mDbFragmentsForSearchBatch(List<string> dbLocationsToProcess,
                Dictionary<string, List<Protein>> dbToMonoMapping, Dictionary<Protein, (string db, int index)> monoToDbAndIndexMapping,
                Dictionary<Protein, byte[]> mutableMonoToUnpairedA3mFragmentMapping)
            {
                var resultTasksMapping = new List<(string db, Task<List<(byte[] data, int index)>> resultTask)>();
                foreach (var dbLocation in dbLocationsToProcess)
                {
                    var proteinsInThisDb = dbToMonoMapping[dbLocation];
                    var indices = monoToDbAndIndexMapping.Where(x => x.Value.db == dbLocation && proteinsInThisDb.Contains(x.Key))
                        .Select(x => x.Value.index).ToList();

                    // queue up tasks for now don't await one by one
                    var unpairedA3mDb = Path.Join(dbLocation, Settings.PersistedDbMonoModeResultDbName);
                    resultTasksMapping.Add((dbLocation, Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(unpairedA3mDb, indices)));
                }

                await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

                foreach (var (dbPath, resultTask) in resultTasksMapping)
                {
                    var proteinsInThisDb = dbToMonoMapping[dbPath];
                    var indexedData = resultTask.Result;
                    foreach (var (data, index) in indexedData)
                    {
                        var protein = proteinsInThisDb.Single(x => monoToDbAndIndexMapping[x].db == dbPath && monoToDbAndIndexMapping[x].index == index);
                        mutableMonoToUnpairedA3mFragmentMapping.Add(protein, data);
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
        var qdbLookupObject = new MmseqsLookupObject();
        var unpairedA3mDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.A3m_MSA_DB);

        //var monoToNewIdMapping = new Dictionary<Protein, int>();
        //var predictionToNewPairIdMapping = new Dictionary<PredictionTarget, int>();

        foreach (var predictionTarget in predictionBatch)
        {
            var indexList = new List<int>();

            foreach (var targetProtein in predictionTarget.UniqueProteins)
            {
                // take the protein from the prefiltered references, avoiding multiple references to equivalent entity
                var protein = targetMonos.Single(x => x.Id == targetProtein.Id);
                var alignData = monoToAlignFragmentMappings[protein];
                var unpairedA3mData = monoToUnpairedA3mMappings[protein];


                qdbDataDbObject.Add(Encoding.ASCII.GetBytes(protein.Sequence), generatedMonoIndex);
                qdbHeaderDbObject.Add(Encoding.ASCII.GetBytes(protein.Id), generatedMonoIndex);
                alignDbObject.Add(alignData, generatedMonoIndex);
                unpairedA3mDbObject.Add(unpairedA3mData, generatedMonoIndex);
                qdbLookupObject.Add(generatedMonoIndex, protein.Id, generatedPredictionIndex);

                indexList.Add(generatedMonoIndex);
                generatedMonoIndex++;
            }

            mutablePredictionToIndexMapping.Add(predictionTarget, indexList);
            generatedPredictionIndex++;
        }

        var pairQdbDataDbPath = $"{pairedQdb}{Mmseqs.Settings.Mmseqs2Internal_DbDataSuffix}";
        var pairQdbHeaderDbPath = $"{pairedQdb}{Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix}";
        var pairQdbLookupPath = $"{pairedQdb}";
        var alignDbDataDbPath = $"{pairedAlignDb}{Mmseqs.Settings.Mmseqs2Internal_DbDataSuffix}";
        var unpairedA3mDbDataDbPath = $"{unpairedA3mDb}{Mmseqs.Settings.Mmseqs2Internal_DbDataSuffix}";

        var writeTasks = new List<Task>
        {
            qdbDataDbObject.WriteToFileSystemAsync(Mmseqs.Settings, pairQdbDataDbPath),
            qdbHeaderDbObject.WriteToFileSystemAsync(Mmseqs.Settings, pairQdbHeaderDbPath),
            qdbLookupObject.WriteToFileSystemAsync(Mmseqs.Settings,pairQdbLookupPath),
            alignDbObject.WriteToFileSystemAsync(Mmseqs.Settings, alignDbDataDbPath),
            unpairedA3mDbObject.WriteToFileSystemAsync(Mmseqs.Settings, unpairedA3mDbDataDbPath)
        };

        await Task.WhenAll(writeTasks);

        return (pairedQdb, pairedAlignDb, unpairedA3mDb);

    }

    private async Task<string> AutoUniprotCreateAlignDbForPairAsync(string workingDir, string qdbPath, string searchResultDb)
    {
        var localProcessingPath = Path.Join(workingDir, "uniprot_pair");
        Directory.CreateDirectory(localProcessingPath);
        var targetDbPathBase = Settings.Custom["UniprotDbPath"];
        var targetDbPathSeq = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
        var targetDbPathAln = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.Custom["colabFold_ExpandParamsUnirefPair"]} {Mmseqs.PerformanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, Settings.PersistedDbPairModeFirstAlignDbName);
        var alignPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,

        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.Custom["colabFold_Align1ParamsPair"]} {Mmseqs.PerformanceParams}");

        return alignResultDb;
    }

    private async Task<string> AutoUniprotCreateMonoDbAsync(string workingDir, string qdbPath, string searchResultDb, string profileResultDb)
    {
        var localProcessingPath = Path.Join(workingDir, "uniprot_mono");
        Directory.CreateDirectory(localProcessingPath);
        var targetDbPathBase = Settings.Custom["UniprotDbPath"];
        var targetDbPathSeq = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
        var targetDbPathAln = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.Custom["colabFold_ExpandParamsUnirefMono"]} {Mmseqs.PerformanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, $"align");
        var alignPosParams = new List<string>()
        {
            profileResultDb,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.Custom["colabFold_AlignParamsMono"]} {Mmseqs.PerformanceParams}");

        //*******************************************filter*******************************************************
        var filterResultDb = Path.Join(localProcessingPath, $"filter");
        var filterPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            alignResultDb,
            filterResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.filterModule, filterPosParams, $"{Settings.Custom["colabFold_FilterParams"]} {Mmseqs.PerformanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"uniref_mono_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            filterResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.Custom["colabFold_MsaConvertParamsMono"]} {Mmseqs.PerformanceParams}");

        return msaConvertResultDb;

    }

    private async Task<string> AutoUniprotPerformPairingAsync(string workingDir, string qdbPath, string pairedAlignDb)
    {
        var targetDbPathBase = Settings.Custom["UniprotDbPath"];
        var targetDbPathSeq = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;

        var localProcessingPath = Path.Join(workingDir, "pairing");
        Directory.CreateDirectory(localProcessingPath);

        //*******************************************pair 1*******************************************************
        var pair1ResultDb = Path.Join(localProcessingPath, $"pair1");
        var pair1PosParams = new List<string>()
        {
            qdbPath,
            targetDbPathBase,
            pairedAlignDb,
            pair1ResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.pairModule, pair1PosParams, $"{Mmseqs.PerformanceParams}");


        //*******************************************align*******************************************************
        var align2ResultDb = Path.Join(localProcessingPath, $"align2");
        var align2PosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            pair1ResultDb,
            align2ResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, align2PosParams, $"{Settings.Custom["colabFold_Align2ParamsPair"]} {Mmseqs.PerformanceParams}");

        //*******************************************pair 1*******************************************************
        var pair2ResultDb = Path.Join(localProcessingPath, $"pair2");
        var pair2PosParams = new List<string>()
        {
            qdbPath,
            targetDbPathBase,
            align2ResultDb,
            pair2ResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.pairModule, pair2PosParams, $"{Mmseqs.PerformanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"uniref_pair_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            pair2ResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.Custom["colabFold_MsaConvertParamsPair"]} {Mmseqs.PerformanceParams}");

        return msaConvertResultDb;
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
        await Mmseqs.RunMmseqsAsync(Mmseqs.searchModule, searchPosParams, $"{Settings.Custom["colabFold_SearchParamsShared"]} {Mmseqs.PerformanceParams}");

        //*******************************************hack up a profile db*******************************************************
        var profileResultDbOriginal = Path.Join(tempSubfolderForUniprotSearch, expectedGeneratedProfileSubPath);
        var profileResultDb = Path.Join(processingFolderRoot, "profile");

        //***move temp file from search as profile db***
        var movePosParams = new List<string>() { profileResultDbOriginal, profileResultDb };
        await Mmseqs.RunMmseqsAsync(Mmseqs.moveModule, movePosParams, String.Empty);

        //***link to header db of qdb since it has the same values***
        var (success, path) = await Mmseqs.RunLinkDbAsync(qdbPath + Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix,
            profileResultDb + Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix);

        if (!success)
        {
            throw new Exception();
        }

        return (searchResultDb, profileResultDb);

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

    private List<List<Protein>> GetBatches(List<Protein> proteins)
    {
        return GetBatches<Protein>(proteins,
            Settings.MaxDesiredBatchSize);
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

    private async Task<(List<PredictionTarget> existing, List<PredictionTarget> missing)> GetExistingAndMissingPredictionTargetsAsync(List<PredictionTarget> targetPredictions, IEnumerable<string> existingDatabaseLocations)
    {
        var expectedExtension = Settings.PersistedDbFinalA3mExtension;

        List<(string expectedFilename, PredictionTarget target)> analyzedSet = targetPredictions
            .Select(x=> (Helper.GetAutoHashIdWithoutMultiplicity(x) + expectedExtension, x))
        .ToList();
        var existing = new List<PredictionTarget>();
        
        // each separate database location that has actual entries inside
        foreach (var location in existingDatabaseLocations)
        {
            // they are organized in subfolders containing the first x symbols of the hash (x=2 2023-04-11 by default, defined in Settings)
            var foldersInThisPath = Directory.GetDirectories(location);

            foreach (var folder in foldersInThisPath)
            {
                if (!analyzedSet.Any()) goto GOTO_MARK_FINALIZE;
                var filesInThisPath = (await Task.Run(() => Directory.GetFiles(folder))).Where(x=>x.EndsWith(expectedExtension));

                foreach (var file in filesInThisPath)
                {
                    if (!analyzedSet.Any()) goto GOTO_MARK_FINALIZE;
                    var index = analyzedSet.FindIndex(x => x.expectedFilename.Equals(file));
                    var found = index >= 0;
                    if (found)
                    {
                        existing.Add(analyzedSet[index].target);
                        analyzedSet.RemoveAt(index);
                    }
                }
            }
        }

        GOTO_MARK_FINALIZE:
        var missing = analyzedSet.Select(x=>x.target).ToList();
        return (existing, missing);
    }

    private async Task<(List<Protein> existing, List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> existingDatabaseLocations, IEnumerable<string> excludeIds)
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

        var dbPaths = await GetDbEntryFoldersAsync(existingDatabaseLocations.ToList());

        var (existing, missing) =
            await GetExistingAndMissingSetsAsync(uniqueProteins, dbPaths);

        return (existing, missing);

    }

    private async Task<(List<Protein> existing, List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<Protein> iproteins, IEnumerable<string> existingDatabaseLocations)
    {
        var proteins = new HashSet<Protein>(iproteins, new ProteinByIdComparer());

        var existing = new List<Protein>();

        var useExistingImplemented = true;
        if (useExistingImplemented)
        {
            foreach (var existingDatabasePath in existingDatabaseLocations)
            {
                //TODO: many checks - whether the sequence matches, whether the other stuff apart from qdb exists, ...
                var qdbHeaderDb = Path.Join(existingDatabasePath, Settings.PersistedDbQdbName) +
                                  $"{Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix}";

                var headers = await Mmseqs.GetAllHeadersInSequenceDbHeaderDbAsync(qdbHeaderDb);
                var contained = proteins.Where(x => headers.Contains(Helper.GetMd5Hash(x.Sequence)));
                existing.AddRange(contained);
            }

        }

        var missing = proteins.Except(existing).ToList();

        return (existing, missing);
    }

    private List<List<PredictionTarget>> GetPredictionTargetBatches(List<PredictionTarget> predictionTargets)
    {
        return GetBatches<PredictionTarget>(predictionTargets,
            Settings.MaxDesiredPredictionTargetBatchSize);
    }
    private bool IsValidDbFolder(string path)
    {
        if (!Directory.Exists(path)) return false;
        if (Directory.GetFiles(path).Length < Settings.PersistedDbMinimalNumberOfFilesInMonoDbResult) return false;
        return true;
    }

    private void LogSomething(string s)
    {
        // var timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
        // Console.WriteLine($"[{timeStamp}]: {s}");
        _logger?.LogInformation(s);
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

}