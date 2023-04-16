﻿using AlphafoldPredictionLib;
using FastaHelperLib;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;

namespace MmseqsHelperLib;

public class ColabfoldMmseqsHelper
{
    const int hardcodedSearchBatchSize = 20;

    private readonly ILogger<ColabfoldMmseqsHelper> _logger;

    public ColabfoldMmseqsHelper(AutoColabfoldMmseqsSettings? inputSettings, ILogger<ColabfoldMmseqsHelper> logger, MmseqsHelper mmseqsHelper)
    {
        _logger = logger;
        Settings = inputSettings ?? GetDefaultSettings();
        Mmseqs = mmseqsHelper;

        var uniprotDbPath = Settings.Custom["UniprotDbPath"];
        var envDbPath = Settings.Custom["EnvDbPath"];

        var uniprotDbName = Path.GetFileName(uniprotDbPath);
        var envDbName = Path.GetFileName(envDbPath);

        var uniprotDb = new MmseqsSourceDatabase(uniprotDbName, uniprotDbPath, new MmseqsSourceDatabaseFeatures(HasTaxonomyData: true));
        var envDb = new MmseqsSourceDatabase(envDbName, envDbPath, new MmseqsSourceDatabaseFeatures(HasTaxonomyData:false));

        //TODO: definitely move this to external config asap. we can't be initializing stuff hardcoded
        ReferenceSourceDatabaseTarget = new MmseqsSourceDatabaseTarget(uniprotDb, true, true);
        var envDbTarget = new MmseqsSourceDatabaseTarget(envDb, true, false);

        MmseqsSourceDatabaseTargets = new List<MmseqsSourceDatabaseTarget>()
        {
            ReferenceSourceDatabaseTarget,
            envDbTarget
        };

        const string hardCodedVersionForNow = "0.0.2.230416";
        Settings.ColabfoldMmseqsHelperDatabaseVersion = hardCodedVersionForNow;

        // shady. TODO: rethink this
#if DEBUG
        Settings.MmseqsVersion = "fake_version_1.0.0";
#else
        Settings.MmseqsVersion = Mmseqs.GetVersionAsync().GetAwaiter().GetResult();
#endif

    }

    public MmseqsHelper Mmseqs { get; }
    public List<MmseqsSourceDatabaseTarget> MmseqsSourceDatabaseTargets { get; }

    public MmseqsSourceDatabaseTarget ReferenceSourceDatabaseTarget { get; }
    public AutoColabfoldMmseqsSettings Settings { get; set; }

    public static AutoColabfoldMmseqsSettings GetDefaultSettings() => new ();

    public async Task GenerateA3msFromFastasGivenExistingMonoDbsAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> persistedMonoDbPaths, IEnumerable<string> excludedPredictionHashes, string outputPath, IEnumerable<string> persistedA3mPaths)
    {
        //TODO: write out log files containing the identity of missing monos or targets

        var excludedHashList = excludedPredictionHashes.ToList();
        var inputPathsList = inputFastaPaths.ToList();
        var persistedA3mPathsList = persistedA3mPaths.ToList();

        var persistedMonoDbPathsList = persistedMonoDbPaths.ToList();
        var filteredPersistedMonoDbPaths = await GetDbEntryFoldersAsync(persistedMonoDbPathsList);

        if (!filteredPersistedMonoDbPaths.Any())
        {
            _logger.LogError("No valid folders found in provided source locations. Need pre-generated mono results to generate pair results.");
            return;
        }
        _logger.LogInformation($"Found {filteredPersistedMonoDbPaths.Count} valid sources of mono predictions. Will proceed.");

        // rectify all targets, giving them standard ordering, capitalization, and referencing the same set of Protein instances
        var rectifiedPredictionTargets = await GetRectifiedTargetPredictionsAsync(inputPathsList, excludedHashList);
        var (existingTargets, missingTargets) = await GetExistingAndMissingPredictionTargetsAsync(rectifiedPredictionTargets, persistedA3mPathsList);

        if (!missingTargets.Any())
        {
            _logger.LogInformation("All targets already exist. No need to calculate any new MSAs.");
            return;
        }
        if (existingTargets.Any())
        {
            _logger.LogInformation($"{existingTargets.Count}/{existingTargets.Count + missingTargets.Count} targets already exist, will not recalculate those.");
        }

        var predictionsPerMonomerCount = GroupPredictionsByNumberOfMonomers(missingTargets);
        foreach (var (numberOfMonomers, targetList) in predictionsPerMonomerCount)
        {
            _logger.LogInformation($"Number of missing predictions containing {numberOfMonomers} unique monomers: {targetList.Count}");
        }

        // all predictions use same mono references (are "rectified"), distinct by reference is ok
        var allMonos = missingTargets.SelectMany(x => x.UniqueProteins).Distinct().ToList();
        var (existingMonos, missingMonos) = await GetExistingAndMissingSetsAsync(allMonos, filteredPersistedMonoDbPaths);

        if (existingMonos.Any())
        {
            _logger.LogInformation($"Found {existingMonos.Count}/{allMonos.Count} monos required for MSA assembly.");
        }
        if (missingMonos.Any())
        {
            _logger.LogWarning($"Some required monos ({missingMonos.Count}/{allMonos.Count}) required for MSA assembly not found, some predictions will be skipped)");
        }

        var targetsMissingMonos = missingTargets
            .Where(x => x.UniqueProteins.Any(pr => missingMonos.Contains(pr))).ToList();
        var predictableTargets = missingTargets.Except(targetsMissingMonos).ToList();
        
        if (targetsMissingMonos.Any())
        {
            _logger.LogWarning($"Some of the provided targets don't have monos required for MSA assembly {targetsMissingMonos.Count}/{missingTargets.Count})");
        }
        if (!predictableTargets.Any())
        {
            _logger.LogWarning("No further targets can be predicted due to missing mono MSA data in the provided locations.");
            return;
        }

        _logger.LogInformation($"Will attempt to generate MSA results for {predictableTargets.Count} targets.");

        var allSuccessful = new List<PredictionTarget>();

        var batches = GetBatches(predictableTargets, Settings.MaxDesiredPredictionTargetBatchSize); 
        foreach (var targetBatch in batches)
        {
            var successfulList = await GenerateA3msFromFastasGivenExistingMonoDbsAsync(outputPath, filteredPersistedMonoDbPaths, targetBatch);
            allSuccessful.AddRange(successfulList);
        }

        _logger.LogInformation($"Number of MSA predictions carried out: {allSuccessful.Count}");

    }

    private List<(int numberOfMonomers, List<PredictionTarget> predictionTargets)> GroupPredictionsByNumberOfMonomers(List<PredictionTarget> targets)
    {
        var res = new Dictionary<int, List<PredictionTarget>>();

        foreach (var predictionTarget in targets)
        {
            var monomerCount = predictionTarget.UniqueProteins.Count;
            if (!res.ContainsKey(monomerCount)) res.Add(monomerCount, new List<PredictionTarget>());
            res[monomerCount].Add(predictionTarget);
        }

        return res.Select(x=>(x.Key, x.Value)).ToList();
    }

    public async Task GenerateColabfoldMonoDbsFromFastasAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> persistedMonoDatabaseParentFolderLocations, IEnumerable<string> excludedIds, string outputPath)
    {
        var excludedIdList = excludedIds.ToList();
        var persistedMonoDbPaths = await GetDbEntryFoldersAsync(persistedMonoDatabaseParentFolderLocations.ToList());

        var uniqueTargets = await GetProteinTargetsForMonoDbSearchAsync(inputFastaPaths, excludedIdList);
        var requiredFeatures = GetRequiredMonoDbFeaturesForTargets(uniqueTargets);

        int existingDbParallelSearchBatchSize = hardcodedSearchBatchSize;
        var (missingFeatures, _) =
            await GetUsableAndMissingFeaturesAsync(persistedMonoDbPaths, requiredFeatures, existingDbParallelSearchBatchSize);

        //TODO: this is a stupid way to do it, recalculating the whole target mono is a single feature is missing. Fix later to redo only the actually missing features.
        var missingTargets = missingFeatures.Select(x => x.Mono).Distinct().ToList();
        var existingTargets = uniqueTargets.Except(missingTargets).ToList();

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

        var batches = GetBatches<Protein>(missingTargets, Settings.MaxDesiredMonoBatchSize);

        foreach (var proteinBatch in batches)
        {
            await GenerateColabfoldMonoDbsForProteinBatchAsync(outputPath, proteinBatch);
        }
    }

    private async Task<(List<MmseqsPersistedMonoDbEntryFeature> missingFeatures, List<MmseqsPersistedMonoDbEntryFeature> usableFeatures)> 
        GetUsableAndMissingFeaturesAsync(List<string> persistedMonoDatabasesPaths, List<Protein> predictionBatch, int existingDbParallelSearchBatchSize)
    {
        var requiredFeatures = GetRequiredMonoDbFeaturesForTargets(predictionBatch);

        var searchBatches = GetBatches<string>(persistedMonoDatabasesPaths, existingDbParallelSearchBatchSize);
        foreach (var searchBatch in searchBatches)
        {
            var featuresToBeFound =
                requiredFeatures.Where(x => !string.IsNullOrWhiteSpace(x.FeatureSubFolderPath)).ToList();
            var monosThatStillNeedToBeFound = featuresToBeFound.Select(x => x.Mono).Distinct().ToList();

            var monoToLocationsMapping =
                await GetMonoToDbAndIndexMappingsForSearchBatch(searchBatch, monosThatStillNeedToBeFound);

            var foundMonos = monoToLocationsMapping.Select(x => x.Key).ToList();
            foreach (var feature in featuresToBeFound.Where(x => foundMonos.Contains(x.Mono)))
            {
                var locationsContainingMono = monoToLocationsMapping[feature.Mono].Select(x => x.dbLocation);
                foreach (var location in locationsContainingMono)
                {
                    var subFoldersInLocation = await Helper.GetDirectoriesAsync(location);
                    var expectedSubFolder = feature.DatabaseName;
                    var matchingSubFolders = subFoldersInLocation.Where(x =>
                        Helper.GetStandardizedDbName(Path.GetFileName(x)) ==
                        Helper.GetStandardizedDbName(expectedSubFolder)).ToList();

                    if (matchingSubFolders.Any())
                    {
                        if (matchingSubFolders.Count > 1)
                        {
                            _logger.LogWarning($"Something is weird with folder naming in ({location})");
                            if (Settings.Strategy.SuspiciousData == SuspiciousDataStrategy.PlaySafe)
                            {
                                _logger.LogWarning($"Will not use it.");
                                continue;
                            }
                        }

                        var subFolderForTargetDatabase = matchingSubFolders.First();
                        var subPath = Path.Join(location, subFolderForTargetDatabase);
                        string requiredDbName;
                        switch (feature.SourceType)
                        {
                            case ColabfoldMsaDataType.Unpaired:
                                requiredDbName = Settings.PersistedDbMonoModeResultDbName;
                                break;
                            case ColabfoldMsaDataType.Paired:
                                requiredDbName = Settings.PersistedDbPairModeFirstAlignDbName;
                                break;
                            default:
                                throw new Exception("This should never happen.");
                        }

                        var requiredDbFileName = $"{requiredDbName}{Mmseqs.Settings.Mmseqs2Internal_DbTypeSuffix}";

                        var files = await Helper.GetFilesAsync(subPath);
                        if (files.Any(x => Path.GetFileName(x) == requiredDbFileName))
                        {
                            feature.DbPath = location;
                            feature.FeatureSubFolderPath = subFolderForTargetDatabase;
                            feature.Indices = monoToLocationsMapping[feature.Mono].Single(x => x.dbLocation == location)
                                .qdbIndices;
                            break;
                        }
                    }
                }
            }
        }

        var missingFeatures = requiredFeatures.Where(x => string.IsNullOrWhiteSpace(x.FeatureSubFolderPath)).ToList();
        var usableFeatures = requiredFeatures.Except(missingFeatures).ToList();

        return (missingFeatures, usableFeatures);
    }

    private List<MmseqsPersistedMonoDbEntryFeature> GetRequiredMonoDbFeaturesForTargets(List<Protein> proteins)
    {
        var res = new List<MmseqsPersistedMonoDbEntryFeature>();
        foreach (var protein in proteins)
        {
            foreach (var dbTarget in MmseqsSourceDatabaseTargets)
            {
                if (dbTarget.UseForUnpaired) res.Add(new MmseqsPersistedMonoDbEntryFeature(protein, dbTarget.Database.Name, ColabfoldMsaDataType.Unpaired));
                if (dbTarget.UseForPaired) res.Add(new MmseqsPersistedMonoDbEntryFeature(protein, dbTarget.Database.Name, ColabfoldMsaDataType.Paired));
            }
        }
        return res;
    }

    private async IAsyncEnumerable<ColabFoldMsaObject> AutoCreateColabfoldMsaObjectsAsync(List<PredictionTarget> predictions, MmseqsDbLocator locator)
    {
        var targetsWithPairing = GetPredictionsThatRequirePairing(predictions);

        foreach (var predictionTarget in predictions)
        {
            List<AnnotatedMsaData> dataEntries = new List<AnnotatedMsaData>();

            var qdbIndices = locator.QdbIndicesMapping[predictionTarget];

            foreach (var dbTarget in MmseqsSourceDatabaseTargets)
            {
                var pairedDataCollection = dbTarget.UseForPaired ? 
                    await Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(locator.PairedA3mDbPathMapping[dbTarget], qdbIndices) : null;
                var unpairedDataCollection = dbTarget.UseForUnpaired ? 
                    await Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(locator.UnPairedA3mDbPathMapping[dbTarget], qdbIndices) : null;

                var unpairedDataDict = new Dictionary<Protein, byte[]>();
                var pairedDataDict = new Dictionary<Protein, byte[]>();

                // TODO: fix this, it's loading stuff part by part while the info expects en-block for each dbTarget
                for (int i = 0; i < predictionTarget.UniqueProteins.Count; i++)
                {
                    var protein = predictionTarget.UniqueProteins[i];

                    if (dbTarget.UseForUnpaired)
                    {
                        var unpairedData = unpairedDataCollection!.Single(x => x.index == qdbIndices[i]).data;
                        unpairedDataDict.Add(protein, unpairedData);
                    }

                    if (dbTarget.UseForPaired && targetsWithPairing.Contains(predictionTarget))
                    {
                        var pairedData = pairedDataCollection!.Single(x => x.index == qdbIndices[i]).data;
                        pairedDataDict.Add(protein, pairedData);
                    }
                }

                dataEntries.Add(new AnnotatedMsaData(ColabfoldMsaDataType.Unpaired, dbTarget, unpairedDataDict));
                dataEntries.Add(new AnnotatedMsaData(ColabfoldMsaDataType.Paired, dbTarget, pairedDataDict));
            }
            
            var msaObj = new ColabFoldMsaObject(dataEntries, predictionTarget);
            yield return msaObj;
        }
    }

    private async Task<string> GenerateSpecialMonoA3mDbForReferenceDbAsync(string workingDir, string qdbPath, string profileResultDb, string searchResultDb, MmseqsSourceDatabaseTarget refDbTarget)
    {
        var localProcessingPath = Path.Join(workingDir, refDbTarget.Database.Name, "mono");
        await Helper.CreateDirectoryAsync(localProcessingPath);
        var targetDbPathBase = refDbTarget.Database.Path;

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.ColabFold_ExpandParamsUnirefMono} {Mmseqs.PerformanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, $"align");
        var alignPosParams = new List<string>()
        {
            profileResultDb,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.ColabFold_AlignParamsMono} {Mmseqs.PerformanceParams}");

        //*******************************************filter*******************************************************
        var filterResultDb = Path.Join(localProcessingPath, $"filter");
        var filterPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            alignResultDb,
            filterResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.filterModule, filterPosParams, $"{Settings.ColabFold_FilterParams} {Mmseqs.PerformanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"mono_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            filterResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.ColabFold_MsaConvertParamsMono} {Mmseqs.PerformanceParams}");

        return msaConvertResultDb;
    }

    private async Task<string> GenerateMonoA3mDbAsync(string workingDir, string qdbPath, string profileDbPath, string searchDbPath, MmseqsSourceDatabaseTarget dbTarget)
    {
        var localProcessingPath = Path.Join(workingDir, dbTarget.Database.Name, "mono");
        await Helper.CreateDirectoryAsync(localProcessingPath);
        var targetDbPathBase = dbTarget.Database.Path;
        var targetDbPathSeq = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;
        var targetDbPathAln = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedAlnDbSuffix;

        //*******************************************search*******************************************************
        var tempSubfolderForSearch = Path.Join(localProcessingPath, "tmp");
        await Helper.CreateDirectoryAsync(tempSubfolderForSearch);

        //*******************************************expand*******************************************************
        var expandResultDb = Path.Join(localProcessingPath, $"expand");
        var expandPosParams = new List<string>()
        {
            profileDbPath,
            targetDbPathSeq,
            searchDbPath,
            targetDbPathAln,
            expandResultDb
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.ColabFold_ExpandParamsEnvMono} {Mmseqs.PerformanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, $"align");
        var alignPosParams = new List<string>()
        {
            profileDbPath,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.ColabFold_AlignParamsMono} {Mmseqs.PerformanceParams}");

        //*******************************************filter*******************************************************
        var filterResultDb = Path.Join(localProcessingPath, $"filter");
        var filterPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            alignResultDb,
            filterResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.filterModule, filterPosParams, $"{Settings.ColabFold_FilterParams} {Mmseqs.PerformanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"mono_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            filterResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.ColabFold_MsaConvertParamsMono} {Mmseqs.PerformanceParams}");

        return msaConvertResultDb;
    }

    private async Task<string> AutoMergeMonoDbsAsync(string workingDir, string qdb, string uniprotMonoDb, string envDbMonoDb)
    {
        var localProcessingPath = Path.Join(workingDir, "final");
        await Helper.CreateDirectoryAsync(localProcessingPath);

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

    private async Task GenerateColabfoldMonoDbsForProteinBatchAsync(string outputBasePath, List<Protein> proteinBatch)
    {
        var batchId = Guid.NewGuid().ToString();

        var workingDir = Path.Join(Settings.TempPath, batchId);
        var outDir = Path.Join(outputBasePath, batchId);
        
        await Helper.CreateDirectoryAsync(workingDir);
        await Helper.CreateDirectoryAsync(outDir);

        LogSomething($"starting batch {batchId} with {proteinBatch.Count} items");
        
        //*******************************************create source fasta file*******************************************************
        var qdbWorkingDir = Path.Join(workingDir, "qdb");
        await Helper.CreateDirectoryAsync(qdbWorkingDir);
        var fastaName = $"input{Settings.FastaSuffix}";
        var queryFastaPath = Path.Join(qdbWorkingDir, fastaName);
        using (var fastaFileOutputStream = File.Create(queryFastaPath))
        {
            await foreach (var fastaChunk in FastaHelper.GenerateMultiFastaDataAsync(proteinBatch))
            {
                await fastaFileOutputStream.WriteAsync(Encoding.ASCII.GetBytes(fastaChunk));
            }
        }

        //*******************************************create query db file*******************************************************
        var qdbNameBase = $"{Settings.PersistedDbQdbName}";
        var qdbPath = Path.Join(qdbWorkingDir, qdbNameBase);

        var createDbParameters = new CreateDbParameters();
        createDbParameters.ApplyDefaults();
        await Mmseqs.CreateDbAsync(queryFastaPath, qdbPath, createDbParameters);

        //*******************************************initial search to get search & profile dbs, using reference database ****************************
        var (refSearchDb, refProfileDb) = await Task.Run(() => GenerateReferenceSearchAndCreateProfileDbAsync(ReferenceSourceDatabaseTarget, workingDir, qdbPath));
        

        //*******************************************calc mono and pair dbs*******************************************************
        var refProcessingTasks = new List<(ColabfoldMsaDataType msaType, Task<string> task)>();

        if (ReferenceSourceDatabaseTarget.UseForUnpaired)
        {
            refProcessingTasks.Add((ColabfoldMsaDataType.Unpaired,
                Task.Run(() => GenerateSpecialMonoA3mDbForReferenceDbAsync(workingDir, qdbPath, refProfileDb, refSearchDb, ReferenceSourceDatabaseTarget))));
        }
        if (ReferenceSourceDatabaseTarget.UseForPaired)
        {
            refProcessingTasks.Add((ColabfoldMsaDataType.Paired,
                Task.Run(() =>
                    GenerateAlignDbForPairingAsync(workingDir, qdbPath, refSearchDb, ReferenceSourceDatabaseTarget))));
        }
        
        var otherDbProcessingTasks = new List<(MmseqsSourceDatabaseTarget dbTarget, Task<(string unpairedDb, string pairedDb)> task)>();

        // reference one is handled in a special way above
        foreach (var dbTarget in MmseqsSourceDatabaseTargets.Where(x=>x!=ReferenceSourceDatabaseTarget))
        {
            otherDbProcessingTasks.Add((dbTarget,
                Task.Run(() => GenerateDbsForTagetSourceDbAsync(dbTarget, qdbPath, refProfileDb, workingDir))));
        }

        var allTasks = refProcessingTasks.Select(x => (Task)x.task).Concat(otherDbProcessingTasks.Select(x => (Task)x.task));

        await Task.WhenAll(allTasks);

        ////*******************************************merge the mono dbs*******************************************************
        //var monoDbPath = referenceDbMonoTask.Result;
        //var pairDbPath = uniprotPairTask.Result;
        //var envDbPath = envDbMonoTask.Result;

        //var mergeMonoDbPath = await AutoMergeMonoDbsAsync(workingDir, qdbPath, monoDbPath, envDbPath);


        //*******************************************copy the result  files to final output*************************************

        var copyTasks = new List<Task>();
        
        var finalPathQdb = Path.Join(outDir, Settings.PersistedDbQdbName);
        copyTasks.Add(Mmseqs.CopyDatabaseAsync(qdbPath, finalPathQdb));
        _logger.LogInformation($"Query db output: {finalPathQdb}");
        
        if (ReferenceSourceDatabaseTarget.UseForUnpaired)
        {
            var monoDbPath = refProcessingTasks.Single(x => x.msaType == ColabfoldMsaDataType.Unpaired).task.Result;
            var finalPathMonos = Path.Join(outDir, ReferenceSourceDatabaseTarget.Database.Name, Settings.PersistedDbMonoModeResultDbName);
            copyTasks.Add(Mmseqs.CopyDatabaseAsync(monoDbPath, finalPathMonos));
            _logger.LogInformation($"Database {ReferenceSourceDatabaseTarget.Database.Name} unpair output: {finalPathMonos}");
        }
        if (ReferenceSourceDatabaseTarget.UseForPaired)
        {
            var alignDbPath = refProcessingTasks.Single(x => x.msaType == ColabfoldMsaDataType.Paired).task.Result;
            var finalPathAlign = Path.Join(outDir, ReferenceSourceDatabaseTarget.Database.Name, Settings.PersistedDbPairModeFirstAlignDbName);
            copyTasks.Add(Mmseqs.CopyDatabaseAsync(alignDbPath, finalPathAlign));
            _logger.LogInformation($"Database {ReferenceSourceDatabaseTarget.Database.Name} align for pair output: {finalPathAlign}");
        }

        foreach (var dbTarget in MmseqsSourceDatabaseTargets.Where(x=>x!=ReferenceSourceDatabaseTarget))
        {
            var paths = otherDbProcessingTasks.Single(x => x.dbTarget == dbTarget).task.Result;
            if (dbTarget.UseForUnpaired)
            {
                var monoDbPath = paths.unpairedDb;
                var finalPathMonos = Path.Join(outDir, dbTarget.Database.Name, Settings.PersistedDbMonoModeResultDbName);
                copyTasks.Add(Mmseqs.CopyDatabaseAsync(monoDbPath, finalPathMonos));
                _logger.LogInformation($"Database {dbTarget.Database.Name} unpair output: {finalPathMonos}");
            }
            if (dbTarget.UseForPaired)
            {
                var alignDbPath = paths.pairedDb;
                var finalPathAlign = Path.Join(outDir, dbTarget.Database.Name, Settings.PersistedDbPairModeFirstAlignDbName);
                copyTasks.Add(Mmseqs.CopyDatabaseAsync(alignDbPath, finalPathAlign));
                _logger.LogInformation($"Database {dbTarget.Database.Name} align for pair output: {finalPathAlign}");
            }
        }

        await Task.WhenAll(copyTasks);

        //******************************************* print out the database info *************************************
        var info = new PersistedMonoDbMetadataInfo(createTime: DateTime.Now,
            referenceDbTarget: ReferenceSourceDatabaseTarget, databaseTargets: MmseqsSourceDatabaseTargets,
            mmseqsHelperDatabaseVersion: Settings.ColabfoldMmseqsHelperDatabaseVersion, targetCount: proteinBatch.Count,
            mmseqsVersion: Settings.MmseqsVersion);

        var infoPath = Path.Join(outDir, Settings.PersistedMonoDbInfoName);
        await info.WriteToFileSystemAsync(infoPath);

    }
    
    private async Task<(string unpairedA3mDb, string forPairAlignDb)> 
        GenerateDbsForTagetSourceDbAsync(MmseqsSourceDatabaseTarget dbTarget, string qdbPath, string refProfileDb, string workingDir)
    {
        var searchDb = await Task.Run(() => GenerateSearchDbAsync(dbTarget, workingDir, refProfileDb));

        var mappedTasks = new Dictionary<ColabfoldMsaDataType, Task<string>>();

        if (dbTarget.UseForUnpaired)
        {
            mappedTasks.Add(ColabfoldMsaDataType.Unpaired, Task.Run(() => GenerateMonoA3mDbAsync(workingDir, qdbPath, refProfileDb, searchDb, dbTarget)));
        }
        if (dbTarget.UseForPaired)
        {
            mappedTasks.Add(ColabfoldMsaDataType.Paired, Task.Run(() => GenerateAlignDbForPairingAsync(workingDir, qdbPath, searchDb, dbTarget)));
        };

        await Task.WhenAll(mappedTasks.Values);

        var unpairedA3mDb = dbTarget.UseForUnpaired ? mappedTasks[ColabfoldMsaDataType.Unpaired].Result : String.Empty;
        var forPairAlignDb = dbTarget.UseForPaired ? mappedTasks[ColabfoldMsaDataType.Paired].Result : String.Empty;
        
        return (unpairedA3mDb, forPairAlignDb); 
    }


    /// <summary>
    /// We are simulating what would happen if we were running it from scratch. For each db used for unpaired, compile a mono a3m db from existing results,
    // that maps to the new constructed qdb indices.
    // For each db that is used for pairing, construct an align1 db from existing results.
    // In original colabfold, uniref and envdb are used for monos, and uniref only is used for pairing. There, also the unpaired/mono reads are pre-combined,
    // prior to combination with the pair result. We are not doing that here as it adds an unnecessary step and lowers the modularity.
    /// </summary>
    /// <param name="workingDir"></param>
    /// <param name="persistedMonoDatabasesPaths"></param>
    /// <param name="predictionBatch"></param>
    /// <returns></returns>
    private async Task<(MmseqsDbLocator locator, List<PredictionTarget> feasiblePredictions)> CompileSourceMonoDbsFromPersistedAsync(
        string workingDir, List<string> persistedMonoDatabasesPaths, List<PredictionTarget> predictionBatch)
    {
        var dbProcessingBatchSize = hardcodedSearchBatchSize;
        
        await Helper.CreateDirectoryAsync(workingDir);
        
        var locator = new MmseqsDbLocator();
        
        //*******************************************figure out which mono dbs contain relevant entries at which indices*******************************************************
        //******************************************* check which persisted dbs used each target mono and have the desired features *******************************************************
        var requiredFeatures = GetRequiredMonoDbFeaturesForPredictions(predictionBatch);
        var (missingFeatures, usableFeatures) = 
            await GetUsableAndMissingFeaturesAsync(persistedMonoDatabasesPaths, requiredFeatures, dbProcessingBatchSize);

        var unfeasibleMonos = missingFeatures.Select(x => x.Mono).ToList();
        List<PredictionTarget> unfeasiblePredictions = new List<PredictionTarget>();
        if (missingFeatures.Any())
        {
            unfeasiblePredictions = predictionBatch
                .Where(x => x.UniqueProteins.Any(prot => unfeasibleMonos.Contains(prot))).ToList();
            
            if (unfeasiblePredictions.Count < predictionBatch.Count)
            {
                _logger.LogWarning(
                    $"Some predictions in batch ({unfeasiblePredictions.Count}/{predictionBatch.Count}) require predicted mono features that are missing. They will be skipped.");
            }
            else
            {
                _logger.LogWarning($"All require predicted mono features that are missing. Aborting batch.");
                return (locator, new List<PredictionTarget>());
            }
        }

        var predictionsToExtractDataFor = predictionBatch.Except(unfeasiblePredictions).ToList();
        //var predictionsForPairing = GetPredictionsThatRequirePairing(predictionsToExtractDataFor);
        var predictionsForPairing = predictionsToExtractDataFor;

        //TODO: need to separate a qdb that includes pairing and qdb that doesn't use pairing
        //******************************************* generate new qdb *******************************************************
        var predictionToIndicesMapping = GeneratePredictionToIndexMapping(predictionsToExtractDataFor);
        var queryDatabaseForPairing = GenerateQdbForPairing(predictionsToExtractDataFor, predictionToIndicesMapping);
        locator.QdbIndicesMapping = predictionToIndicesMapping;

        var pairedQdb = Path.Join(workingDir, "qdb_pair");
        locator.PairingQdbPath = pairedQdb;
        var writeTasks = new List<Task> {
            queryDatabaseForPairing.WriteToFileSystemAsync(Mmseqs.Settings, pairedQdb)
        };

        //******************************************* and read in relevant fragments of align files*******************************************************
        //******************************************* reconstruct align data dbs *******************************************************
        //******************************************* reconstruct mono data dbs ********************************************************

        // we are simulating what would happen if we were running it from scratch. For each db used for unpaired, compile a mono a3m db from existing results,
        // that maps to the new constructed qdb indices.
        // For each db that is used for pairing, construct an align1 db from existing results.
        // In original colabfold, uniref and envdb are used for monos, and uniref only is used for pairing. There, also the unpaired/mono reads are pre-combined,
        // prior to combination with the pair result. We are not doing that here as it adds an unnecessary step and lowers the modularity.
        foreach (var dbTarget in MmseqsSourceDatabaseTargets)
        {
            var featuresForThisSourceDb = usableFeatures
                .Where(x=>x.DatabaseName == dbTarget.Database.Name).ToList();

            var localPath = Path.Join(workingDir, dbTarget.Database.Name);
            await Helper.CreateDirectoryAsync(localPath);
            if (dbTarget.UseForPaired)
            {
                var dataType = ColabfoldMsaDataType.Paired;
                var monoToAlignFragmentMappings = await GenerateDataDbFragmentsForSourceDatabaseAsync(
                    dbTarget.Database, featuresForThisSourceDb, dataType, dbProcessingBatchSize);
                var alignDbObject = GenerateDbObjectForPredictionBatch(predictionsForPairing, monoToAlignFragmentMappings, locator, dataType);

                var pairedAlignDb = Path.Join(localPath, "align1");
                var alignDbDataDbPath = $"{pairedAlignDb}{Mmseqs.Settings.Mmseqs2Internal_DbDataSuffix}";

                writeTasks.Add(alignDbObject.WriteToFileSystemAsync(Mmseqs.Settings, alignDbDataDbPath));
                locator.PrePairingAlignDbPathMapping.Add(dbTarget, pairedAlignDb);
            }

            if (dbTarget.UseForUnpaired)
            {
                var dataType = ColabfoldMsaDataType.Unpaired;
                var monoToUnpairedA3mFragmentMappings = await GenerateDataDbFragmentsForSourceDatabaseAsync(
                    dbTarget.Database, featuresForThisSourceDb, dataType, dbProcessingBatchSize);
                var unpairedA3mDbObject = GenerateDbObjectForPredictionBatch(predictionsToExtractDataFor, monoToUnpairedA3mFragmentMappings, locator, dataType);

                var unpairedA3mDb = Path.Join(localPath, "unpaired_a3m");
                var unpairedA3mDbDataDbPath = $"{unpairedA3mDb}{Mmseqs.Settings.Mmseqs2Internal_DbDataSuffix}";

                writeTasks.Add(unpairedA3mDbObject.WriteToFileSystemAsync(Mmseqs.Settings, unpairedA3mDbDataDbPath));
                locator.UnPairedA3mDbPathMapping.Add(dbTarget, unpairedA3mDb);
            }
        }

        await Task.WhenAll(writeTasks);
        return (locator, predictionsToExtractDataFor);
    }

    private List<PredictionTarget> GetPredictionsThatRequirePairing(List<PredictionTarget> targets)
    {
        //TODO: add support for explicitly asking for no-pairing (possibly per target?)
        return targets.Where(x => x.IsHeteroComplex).ToList();
    }

    private MmseqsDatabaseObject GenerateDbObjectForPredictionBatch(
        List<PredictionTarget> predictionBatch, Dictionary<Protein, byte[]> monoToDataFragmentMappings, MmseqsDbLocator locator, ColabfoldMsaDataType databaseType)
    {
        MmseqsDatabaseType mmseqsDbType;

        switch (databaseType)
        {
            case ColabfoldMsaDataType.Paired:
                mmseqsDbType = MmseqsDatabaseType.Alignment_ALIGNMENT_RES;
                break;
            case ColabfoldMsaDataType.Unpaired:
                mmseqsDbType = MmseqsDatabaseType.A3m_MSA_DB;
                break;
            default: throw new NotImplementedException();
        }

        var mmseqsDbObject = new MmseqsDatabaseObject(mmseqsDbType);
        
        foreach (var predictionTarget in predictionBatch)
        {
            var indices = locator.QdbIndicesMapping[predictionTarget];
            if (indices.Count != predictionTarget.UniqueProteins.Count)
            {
                _logger.LogError("Something is wrong with the processing, unequal number of db indices and unique proteins in a target");
                throw new Exception();
            }
            
            for (int i = 0; i<indices.Count; i++)
            {
                var protein = predictionTarget.UniqueProteins[i];
                var monoIndex = indices[i];
                
                var data = monoToDataFragmentMappings[protein];
                mmseqsDbObject.Add(data, monoIndex);
            }
        }

        return mmseqsDbObject;
    }

    /// <summary>
    /// Contains original data for each protein target, doesn't strip anything except the data terminator \0
    /// </summary>
    /// <param name="targetDb"></param>
    /// <param name="featuresCollection"></param>
    /// <param name="dataType"></param>
    /// <param name="persistedDbLocationBatchSize"></param>
    /// <returns></returns>
    private async Task<Dictionary<Protein, byte[]>> GenerateDataDbFragmentsForSourceDatabaseAsync(
        MmseqsSourceDatabase targetDb, 
        List<MmseqsPersistedMonoDbEntryFeature> featuresCollection,
        ColabfoldMsaDataType dataType,
        int persistedDbLocationBatchSize)
    {
        // actual data of each aligndb for each mono
        var monoToDataFragmentMappings = new Dictionary<Protein, byte[]>();
        
        var relevantFeatures = featuresCollection
            .Where(x=>x.DatabaseName == targetDb.Name && x.SourceType == dataType).ToList();
        var relevantLocations = relevantFeatures
            .Select(x => x.DbPath).Distinct().ToList();

        var relevantLocationBatches = GetBatches(relevantLocations, persistedDbLocationBatchSize);
        foreach (var searchBatch in relevantLocationBatches)
        {
            // the monoToAlignFragmentMappings dict gets mutated each round
            await AppendFragmentDictionaryForSearchBatchAsync(monoToDataFragmentMappings, searchBatch, relevantFeatures, dataType);
        }

        // need to include the empty entries for ones not found (it happens sometimes that after filter or whatever has no results,
        // and this reconstruction is mimicking that normal behavior
        var relevantMonos = relevantFeatures.Select(x => x.Mono);
        foreach (var relevantMono in relevantMonos)
        {
            var hasNoResults = !monoToDataFragmentMappings.ContainsKey(relevantMono);
            if (hasNoResults)
            {
                monoToDataFragmentMappings.Add(relevantMono, Array.Empty<byte>());
            }
        }

        return monoToDataFragmentMappings;
    }

    private async Task<(List<MmseqsPersistedMonoDbEntryFeature> missingFeatures, List<MmseqsPersistedMonoDbEntryFeature> usableFeatures)> 
        GetUsableAndMissingFeaturesAsync(List<string> persistedMonoDatabasesPaths, List<MmseqsPersistedMonoDbEntryFeature> requiredFeatures, int existingDbParallelSearchBatchSize)
    {
        var searchBatches = GetBatches<string>(persistedMonoDatabasesPaths, existingDbParallelSearchBatchSize);
        foreach (var searchBatch in searchBatches)
        {
            var featuresToBeFound =
                requiredFeatures.Where(x => string.IsNullOrWhiteSpace(x.FeatureSubFolderPath)).ToList();
            var monosThatStillNeedToBeFound = featuresToBeFound.Select(x => x.Mono).Distinct().ToList();

            var monoToLocationsMapping =
                await GetMonoToDbAndIndexMappingsForSearchBatch(searchBatch, monosThatStillNeedToBeFound);

            var foundMonos = monoToLocationsMapping.Select(x => x.Key).ToList();
            foreach (var feature in featuresToBeFound.Where(x => foundMonos.Contains(x.Mono)))
            {
                var locationsContainingMono = monoToLocationsMapping[feature.Mono].Select(x => x.dbLocation);
                foreach (var location in locationsContainingMono)
                {
                    var subFoldersInLocation = (await Helper.GetDirectoriesAsync(location)).Select(x=> Path.GetFileName(x)).ToList();
                    var expectedSubFolder = feature.DatabaseName;
                    var matchingSubFolders = subFoldersInLocation.Where(x =>
                        Helper.GetStandardizedDbName(x) ==
                        Helper.GetStandardizedDbName(expectedSubFolder)).ToList();

                    if (matchingSubFolders.Any())
                    {
                        if (matchingSubFolders.Count > 1)
                        {
                            _logger.LogWarning($"Something is weird with folder naming in ({location})");
                            if (Settings.Strategy.SuspiciousData == SuspiciousDataStrategy.PlaySafe)
                            {
                                _logger.LogWarning($"Will not use it.");
                                continue;
                            }
                        }

                        // theoretically there be multiple, should never happen but eh. At this point
                        var subFolderForTargetDatabase = matchingSubFolders.First();
                        var subPath = Path.Join(location, subFolderForTargetDatabase);
                        string requiredDbName;
                        switch (feature.SourceType)
                        {
                            case ColabfoldMsaDataType.Unpaired:
                                requiredDbName = Settings.PersistedDbMonoModeResultDbName;
                                break;
                            case ColabfoldMsaDataType.Paired:
                                requiredDbName = Settings.PersistedDbPairModeFirstAlignDbName;
                                break;
                            default:
                                throw new Exception("This should never happen.");
                        }

                        var requiredDbFileName = $"{requiredDbName}{Mmseqs.Settings.Mmseqs2Internal_DbTypeSuffix}";

                        var files = await Helper.GetFilesAsync(subPath);
                        if (files.Any(x => Path.GetFileName(x) == requiredDbFileName))
                        {
                            feature.DbPath = location;
                            feature.FeatureSubFolderPath = subFolderForTargetDatabase;
                            feature.Indices = monoToLocationsMapping[feature.Mono].Single(x => x.dbLocation == location)
                                .qdbIndices;
                            break;
                        }
                    }
                }
            }
        }

        var missingFeatures = requiredFeatures.Where(x => string.IsNullOrWhiteSpace(x.FeatureSubFolderPath)).ToList();
        var usableFeatures = requiredFeatures.Except(missingFeatures).ToList();

        return (missingFeatures, usableFeatures);
    }

    /// <summary>
    /// Append the mutable dictionary by all the data fragments for relevant features. Will read e.g. 20 persisted mono db locations at once for presence of
    /// (for_pair_align or unpaired) data, depending on input, that belongs to the target source database (read from features?).
    /// ... fragile, hard to understand. Needs to be rewritten.
    ///
    /// Appends data as-is, removing just the data terminator (null ascii), doesn't do anything further regarding newlines etc
    /// </summary>
    /// <param name="mutableMonoToDataFragmentMapping"></param>
    /// <param name="dbLocationsToProcess"></param>
    /// <param name="preprocessedFeatures"></param>
    /// <param name="targetMsaDataType"></param>
    /// <param name="sourceDatabase"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    private async Task AppendFragmentDictionaryForSearchBatchAsync(Dictionary<Protein, byte[]> mutableMonoToDataFragmentMapping,
        List<string> dbLocationsToProcess,
        List<MmseqsPersistedMonoDbEntryFeature> preprocessedFeatures,
        ColabfoldMsaDataType targetMsaDataType
        )
    {
        var resultTasksMapping = new List<(string db, Task<List<(byte[] data, int index)>> resultTask)>();
        foreach (var dbLocation in dbLocationsToProcess)
        {
            // we want the features that match all: (persisted location, source database, type)
            var featuresInThisPersistedDb = preprocessedFeatures
                .Where(x => x.DbPath == dbLocation).ToList();
            // and we want the indices for those features, better if unique. Those indices will be extracted from this (location, db, type).
            var mmseqsIndicesForProteinsInThisPersistedDb = featuresInThisPersistedDb
                .SelectMany(x => x.Indices).Distinct().ToList();

            // depending on type (paired / unpaired data) we expect a different db base name
            string persistedDbName;
            switch (targetMsaDataType)
            {
                case ColabfoldMsaDataType.Paired:
                    persistedDbName = Settings.PersistedDbPairModeFirstAlignDbName;
                    break;
                case ColabfoldMsaDataType.Unpaired:
                    persistedDbName = Settings.PersistedDbMonoModeResultDbName;
                    break;
                default: throw new NotImplementedException("This shouldn't ever happen.");
            }

            // there should be only one matching subfolder? we are looking at features that have the same location, db name, type. So they should map to the same subfolder
            // still this is fragile, therefore TODO: make this better
            var subfolder = featuresInThisPersistedDb.Select(x => x.FeatureSubFolderPath).Distinct().Single();
            string mmseqsDb = Path.Join(dbLocation, subfolder, persistedDbName);
            // queue up the reads
            resultTasksMapping.Add((dbLocation, Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(mmseqsDb, mmseqsIndicesForProteinsInThisPersistedDb)));
        }

        // proceed after all the batched reads are done
        await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

        // now, combine all of these into the actual dict we want, where it's mapped to proteins, not mmseqs db indices like before
        foreach (var (dbLocation, resultTask) in resultTasksMapping)
        {
            var featuresInThisPersistedDb = preprocessedFeatures
                .Where(x => x.DbPath == dbLocation).ToList();
            var indexedData = resultTask.Result;
            foreach (var (data, dbIndex) in indexedData)
            {
                var locatedFeature = featuresInThisPersistedDb.Single(x => x.Indices.Contains(dbIndex));
                var protein = locatedFeature.Mono;
                mutableMonoToDataFragmentMapping.Add(protein, data);
            }
        }
    }

    private Dictionary<PredictionTarget, List<int>>  GeneratePredictionToIndexMapping(List<PredictionTarget> targets)
    {
        var predictionTargetToDbIndicesMapping = new Dictionary<PredictionTarget, List<int>>();
        var generatedMonoIndex = 0;
        
        foreach (var predictionTarget in targets)
        {
            var indexList = new List<int>();

            foreach (var _ in predictionTarget.UniqueProteins)
            {
                indexList.Add(generatedMonoIndex);
                generatedMonoIndex++;
            }
            predictionTargetToDbIndicesMapping.Add(predictionTarget, indexList);
        }
        
        return predictionTargetToDbIndicesMapping;
    }



    private MmseqsQueryDatabaseContainer GenerateQdbForPairing(List<PredictionTarget> targets, Dictionary<PredictionTarget, List<int>> targetToDbIndicesMapping)
    {
        var mmseqsQueryDatabase = new MmseqsQueryDatabaseContainer();

        var generatedPredictionIndex = 0;

        var qdbDataDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.Sequence_AMINO_ACIDS);
        var qdbHeaderDbObject = new MmseqsDatabaseObject(MmseqsDatabaseType.Header_GENERIC_DB);
        var qdbLookupObject = new MmseqsLookupObject();

        foreach (var predictionTarget in targets)
        {
            var indexList = targetToDbIndicesMapping[predictionTarget];

            if (indexList.Count != predictionTarget.UniqueProteins.Count)
                throw new Exception("Faulty input. Mismatch between index and protein counts");

            for (int i = 0; i < predictionTarget.UniqueProteins.Count; i++)
            {
                var monoIndex = indexList[i];
                var protein = predictionTarget.UniqueProteins[i];

                qdbDataDbObject.Add(Encoding.ASCII.GetBytes(protein.Sequence), monoIndex);
                qdbHeaderDbObject.Add(Encoding.ASCII.GetBytes(protein.Id), monoIndex);
                qdbLookupObject.Add(monoIndex, protein.Id, generatedPredictionIndex);
            }

            generatedPredictionIndex++;
        }
        
        mmseqsQueryDatabase.DataDbObject = qdbDataDbObject;
        mmseqsQueryDatabase.HeaderDbObject = qdbHeaderDbObject;
        mmseqsQueryDatabase.LookupObject = qdbLookupObject;
        
        return mmseqsQueryDatabase;
    }

    /// <summary>
    /// Input protein list expected to be unique
    /// </summary>
    /// <param name="dbLocationsToSearch"></param>
    /// <param name="uniqueProteins"></param>
    /// <returns></returns>
    private async Task<Dictionary<Protein, List<(string dbLocation, List<int> qdbIndices)>>> 
        GetMonoToDbAndIndexMappingsForSearchBatch(List<string> dbLocationsToSearch, List<Protein> uniqueProteins)
    {
        var res = new Dictionary<Protein, List<(string dbLocation, List<int> qdbIndices)>>();
        
        var resultTasksMapping = new List<(string dbLocation, Task<List<(string header, List<int> indices)>> resultTask)>();
        foreach (var dbLocation in dbLocationsToSearch)
        {
            // queue up tasks for now don't await one by one
            var qdbPath = Path.Join(dbLocation, Settings.PersistedDbQdbName);
            var headersToSearch = uniqueProteins.Select(x => x.Id).ToList();
            resultTasksMapping.Add((dbLocation, Mmseqs.GetHeaderAndIndicesForGivenHeadersInSequenceDbAsync(qdbPath, headersToSearch)));
        }

        await Task.WhenAll(resultTasksMapping.Select(x => x.resultTask));

        foreach (var (dbPath, resultTask) in resultTasksMapping)
        {
            var entriesInDb = resultTask.Result;
            foreach (var (id, indices) in entriesInDb)
            {
                if (!indices.Any()) continue;
                var protein = uniqueProteins.Single(x => x.Id == id);
                if (!res.ContainsKey(protein))
                {
                    res.Add(protein, new List<(string dbLocation, List<int> qdbIndices)>());
                    
                }
                res[protein].Add((dbPath, indices));
            }
        }

        return res;

    }

    private List<MmseqsPersistedMonoDbEntryFeature> GetRequiredMonoDbFeaturesForPredictions(List<PredictionTarget> predictionBatch)
    {
        var requiringPairing = GetPredictionsThatRequirePairing(predictionBatch);
        var notRequiringPairing = predictionBatch.Except(requiringPairing).ToList();
        
        var proteinsRequiringPairing = requiringPairing.SelectMany(x => x.UniqueProteins).Distinct().ToList();
        var proteinsInPairlessTargets = notRequiringPairing.SelectMany(x => x.UniqueProteins).Distinct().ToList();

        var allProteins = proteinsRequiringPairing.Concat(proteinsInPairlessTargets).Distinct().ToList();
        var allFeatures = GetRequiredMonoDbFeaturesForTargets(allProteins);

        var proteinsNotRequiringPairing = proteinsInPairlessTargets.Except(proteinsRequiringPairing).ToList();
        var notNeededFeatures = allFeatures.Where(x=>x.SourceType == ColabfoldMsaDataType.Paired
        && proteinsNotRequiringPairing.Contains(x.Mono));

        var finalFeatures = allFeatures.Except(notNeededFeatures).ToList();

        return finalFeatures;
    }

    private async Task<string> GenerateAlignDbForPairingAsync(string workingDir, string qdbPath, string searchResultDb,
        MmseqsSourceDatabaseTarget dbTarget)
    {
        var localProcessingPath = Path.Join(workingDir, dbTarget.Database.Name, "pair");
        await Helper.CreateDirectoryAsync(localProcessingPath);
        var targetDbPathBase = dbTarget.Database.Path;
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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.ColabFold_ExpandParamsUnirefPair} {Mmseqs.PerformanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, Settings.PersistedDbPairModeFirstAlignDbName);
        var alignPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,

        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.ColabFold_Align1ParamsPair} {Mmseqs.PerformanceParams}");

        return alignResultDb;
    }
    
    private async Task<string> PerformPairingForDbTargetAsync(string workingDir, string qdbPath, string pairedAlignDb, MmseqsSourceDatabaseTarget dbTarget)
    {
        var targetDbPathBase = dbTarget.Database.Path;
        var targetDbPathSeq = targetDbPathBase + Mmseqs.Settings.Mmseqs2Internal_ExpectedSeqDbSuffix;

        var localProcessingPath = Path.Join(workingDir, dbTarget.Database.Name, "pairing");
        await Helper.CreateDirectoryAsync(localProcessingPath);

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, align2PosParams, $"{Settings.ColabFold_Align2ParamsPair} {Mmseqs.PerformanceParams}");

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
        var msaConvertResultDb = Path.Join(localProcessingPath, $"pair_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            pair2ResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.ColabFold_MsaConvertParamsPair} {Mmseqs.PerformanceParams}");

        return msaConvertResultDb;
    }

    private async Task<string> GenerateSearchDbAsync(MmseqsSourceDatabaseTarget dbTarget,
        string workingDir, string profileDbPath)
    {
        //*******************************************search*******************************************************
        var processingFolderRoot = Path.Join(workingDir, dbTarget.Database.Name, "search");
        await Helper.CreateDirectoryAsync(processingFolderRoot);
        var searchSubfolder = Path.Join(processingFolderRoot, "tmp");
        await Helper.CreateDirectoryAsync(searchSubfolder);
        
        var searchResultDb = Path.Join(processingFolderRoot, $"search");
        var searchPosParams = new List<string>() { profileDbPath, dbTarget.Database.Path, searchResultDb, searchSubfolder };
        await Mmseqs.RunMmseqsAsync(Mmseqs.searchModule, searchPosParams, $"{Settings.ColabFold_SearchParamsShared} {Mmseqs.PerformanceParams}");
        
        return searchResultDb;
    }

    private async Task<(string searchDb, string profileDb)> GenerateReferenceSearchAndCreateProfileDbAsync(MmseqsSourceDatabaseTarget dbTarget, string workingDir, string qdbPath)
    {
        //*******************************************search*******************************************************
        var processingFolderRoot = Path.Join(workingDir, dbTarget.Database.Name, "search");
        await Helper.CreateDirectoryAsync(processingFolderRoot);
        var searchSubfolder = Path.Join(processingFolderRoot, "tmp");
        await Helper.CreateDirectoryAsync(searchSubfolder);
        var expectedGeneratedProfileSubPath = Path.Join("latest", "profile_1");

        var searchResultDb = Path.Join(processingFolderRoot, $"search");
        var searchPosParams = new List<string>() { qdbPath, dbTarget.Database.Path, searchResultDb, searchSubfolder };
        await Mmseqs.RunMmseqsAsync(Mmseqs.searchModule, searchPosParams, $"{Settings.ColabFold_SearchParamsShared} {Mmseqs.PerformanceParams}");

        //*******************************************hack up a profile db*******************************************************
        var profileResultDbOriginal = Path.Join(searchSubfolder, expectedGeneratedProfileSubPath);
        var profileResultDb = Path.Join(processingFolderRoot, "profile");

        //***move temp file from search as profile db***
        var movePosParams = new List<string>() { profileResultDbOriginal, profileResultDb };
        await Mmseqs.RunMmseqsAsync(Mmseqs.moveModule, movePosParams, String.Empty);

        //***link to header db of qdb since it has the same values***
        var (success, path) = await Mmseqs.RunLinkDbAsync(qdbPath + Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix,
            profileResultDb + Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix);

        if (!success)
        {
            throw new Exception("Failed to link the profile db");
        }

        return (searchResultDb, profileResultDb);

    }

    private async Task<List<PredictionTarget>> GenerateA3msFromFastasGivenExistingMonoDbsAsync(string outputPath, List<string> realPersistedMonoDatabasesPaths, List<PredictionTarget> predictionBatch)
    {
        var resultList = new List<PredictionTarget>();

        var batchGuid = Guid.NewGuid();
        var batchId = batchGuid.ToString();
        var shortBatchId = Helper.GetMd5Hash(batchId).Substring(0, Settings.PersistedA3mDbShortBatchIdLength);

        var workingDir = Path.Join(Settings.TempPath, batchId);
        await Helper.CreateDirectoryAsync(workingDir);

        LogSomething($"Starting pairing batch {batchId} with {predictionBatch.Count} items in {workingDir}.");

        //TODO: check if it has all the required dbs: qdb header, (qdb seq => technically not really needed), aligndb, monoa3m
        // not sure where it's best to do this without duplicating the entire search. Probably step-wise, also to allow pair-only mode later

        //*******************************************construct the starting dbs from mono fragments****************************
        //*******************************************grab the relevant mono results*******************************************************
        LogSomething($"Collecting mono data required for pairing...");
        var (dbLocatorObject, feasiblePredictionTargets) = await CompileSourceMonoDbsFromPersistedAsync(workingDir, realPersistedMonoDatabasesPaths, predictionBatch);

        if (!feasiblePredictionTargets.Any())
        {
            _logger.LogWarning("All targets in the batch were unfeasible");
            return resultList;
        }
        if (feasiblePredictionTargets.Count != predictionBatch.Count)
        {
            // var diff = predictionBatch.Count - feasiblePredictionTargets.Count;
            _logger.LogWarning($"Some targets were unfeasible, continuing with ({feasiblePredictionTargets.Count}/${predictionBatch.Count}");
        }

        //*******************************************perform pairing*******************************************************
        LogSomething($"Performing MSA pairing...");
        //TODO: for now I anyhow have only uniref for pairing, but this can be parallelized for all paired dbs, do this parallelization!
        foreach (var dbTarget in MmseqsSourceDatabaseTargets.Where(x=>x.UseForPaired))
        {
            LogSomething($"Performing MSA pairing for {dbTarget.Database.Name}...");
            var prePairAlignDb = dbLocatorObject.PrePairingAlignDbPathMapping[dbTarget];
            var pairedDbPath = await PerformPairingForDbTargetAsync(workingDir, dbLocatorObject.PairingQdbPath, prePairAlignDb, dbTarget);
            dbLocatorObject.PairedA3mDbPathMapping[dbTarget] = pairedDbPath;
        }

        //*******************************************construct individual result dbs*******************************************************
        LogSomething($"Generating final a3m files...");
        //TODO: can already start output of non-pair ones while pairing is running 

        var msaObjectCounter = 0;
        var writeTasks = new List<Task>();
        await foreach (var msaObject in AutoCreateColabfoldMsaObjectsAsync(feasiblePredictionTargets, dbLocatorObject))
        {
            msaObjectCounter++;
            //var autoName = msaObject.HashId;
            var predictionSubFolderPath = GetMsaResultsSubFolderPathForPrediction(msaObject.PredictionTarget);
            var resultSubPath = Path.Join(predictionSubFolderPath, shortBatchId);
            var fullResultPath = Path.Join(outputPath, resultSubPath);

            //*******************************************write the result files*************************************
            var pt = msaObject.PredictionTarget;
            LogSomething($"Writing {Helper.GetMultimerName(pt)} result with total length {pt.TotalLength} in {fullResultPath}...");
            await Helper.CreateDirectoryAsync(fullResultPath);
            writeTasks.Add(msaObject.WriteToFileSystemAsync(Settings, fullResultPath));
            resultList.Add(msaObject.PredictionTarget);
        }
        
        await Task.WhenAll(writeTasks);
        LogSomething($"Wrote {msaObjectCounter} result files in {outputPath}.");

        return resultList;
    }

    private string GetMsaResultsSubFolderPathForPrediction(PredictionTarget target)
    {
        var predictionHash = Helper.GetAutoHashIdWithoutMultiplicity(target);

        var pathFragments =
            Settings.PersistedA3mDbFolderOrganizationFragmentLengths.Select(x => predictionHash.Substring(0, x));
        var subPath = Path.Join(pathFragments.ToArray());
        
        return Path.Join(subPath, predictionHash);
    }

    private List<List<T>> GetBatches<T>(List<T> sourceList, int desiredBatchSize)
    {
        //var batchCount = 1 + (sourceList.Count - 1) / desiredBatchSize;

        var batches = new List<List<T>>();

        //https://stackoverflow.com/a/4262134/4554766
        var rng = new Random();
        var counter = 0;
        var tempList = new List<T>();
        foreach (var target in sourceList.OrderBy(x => rng.Next()))
        {
            tempList.Add(target);
            counter++;
            if (counter == desiredBatchSize)
            {
                batches.Add(tempList);
                tempList = new List<T>();
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

            var foldersInside = await Helper.GetDirectoriesAsync(existingDatabaseFolderPath);
            var foldersWithTask = foldersInside.Select(x=>(folder:x, checkValidityTask: IsValidDbFolder(x))).ToList();
            await Task.WhenAll(foldersWithTask.Select(x => x.checkValidityTask));
            var validFolders = foldersWithTask.Where(x=>x.checkValidityTask.Result).Select(x=>x.folder);
            result.AddRange(validFolders);
        }

        return result;
    }

    private async Task<(List<PredictionTarget> existing, List<PredictionTarget> missing)> 
        GetExistingAndMissingPredictionTargetsAsync(List<PredictionTarget> targetPredictions, IEnumerable<string> existingDatabaseLocations)
    {
        var resultsLocationMappingToTest = new List<(PredictionTarget target, string subpath)>();
        var targetToFullResultMapping = new Dictionary<PredictionTarget, string>();

        foreach (var targetPrediction in targetPredictions)
        {
            resultsLocationMappingToTest.Add((targetPrediction, GetMsaResultsSubFolderPathForPrediction(targetPrediction)));
        }

        // each separate database location that has actual entries inside
        foreach (var location in existingDatabaseLocations)
        {
            if (!resultsLocationMappingToTest.Any()) goto GOTO_MARK_FINALIZE;

            if (!Directory.Exists(location))
            {
                _logger.LogWarning($"Provided location does not exist: {location}");
                continue;
            }
            
            // TODO: parallelize this, this is many disk reads that can work together
            foreach (var (target, subpath) in resultsLocationMappingToTest)
            {
                var expectedPredictionTargetResultsPath = Path.Join(location, subpath);
                if (!Directory.Exists(expectedPredictionTargetResultsPath)) continue;
                
                await foreach (var desiredResultFolder in GetResultFoldersWithDesiredResultAsync(target,
                                   expectedPredictionTargetResultsPath))
                {
                    var firstResult = desiredResultFolder;
                    targetToFullResultMapping[target] = firstResult;
                    break;
                }
            }

            var alreadyFound = targetToFullResultMapping.Select(x => x.Key).ToList();
            resultsLocationMappingToTest.RemoveAll(x => alreadyFound.Contains(x.target));
        }

        // ########################################################
        GOTO_MARK_FINALIZE:

        var targetsWithMissingResults = resultsLocationMappingToTest.Select(x => x.target).ToList();
        var targetsWithExistingResults = targetPredictions.Except(targetsWithMissingResults).ToList();

        return (targetsWithExistingResults, targetsWithMissingResults);
    }

    /// <summary>
    /// Result location for a prediction can have multiple results in it for different combos of inputs. This will get all that fit the input target
    /// and the settings of the current program instance
    /// </summary>
    /// <param name="target"></param>
    /// <param name="fullPredictionPathToCheckForResults"></param>
    /// <returns></returns>
    private async IAsyncEnumerable<string> GetResultFoldersWithDesiredResultAsync(PredictionTarget target, string fullPredictionPathToCheckForResults)
    {
        var subFolders = await Helper.GetDirectoriesAsync(fullPredictionPathToCheckForResults);
        if (!subFolders.Any()) yield break;
        
        foreach (var resultFolder in subFolders)
        {
            //if (await Helper.FilesExistParallelAsync())

            var expectedInfoFilePath = Path.Join(resultFolder, Settings.PersistedDbFinalA3mInfoName);
            if (!File.Exists(expectedInfoFilePath)) continue;

            var expectedMsaFilePath = Path.Join(resultFolder, Settings.PersistedDbFinalA3mName);
            if (!File.Exists(expectedMsaFilePath)) continue;

            bool isAcceptable;
            try
            {
                isAcceptable = await CheckIsAcceptablePersistedA3mResultForTargetAsync(target, expectedInfoFilePath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Error while checking {expectedInfoFilePath}, skipping.");
                continue;
            }

            if (isAcceptable) yield return resultFolder;

        }
    }

    private async Task<bool> CheckIsAcceptablePersistedA3mResultForTargetAsync(PredictionTarget target, string infoFilePath)
    {
        var x = await ColabfoldMsaMetadataInfo.ReadFromFileSystemAsync(infoFilePath);
        if (x is null)
        {
            _logger.LogInformation($"Unable to load the database info for {infoFilePath}, skipping.");
            return false;
        }
        var persistedA3mInfo = x!;

        if (!persistedA3mInfo.PredictionTarget.SameUniqueProteinsAs(target))
        {
            _logger.LogWarning($"Something is wrong with persisted a3m at location {infoFilePath}, constituents not matching expected target. This indicates hash mismatch. Database could be corrupt.");
            return false;
        }

        if (!IsPersistedMmseqsDatabaseVersionCompatible(persistedA3mInfo.MmseqsHelperDatabaseVersion)) return false;
        if (!Settings.Strategy.AllowDifferentMmseqsVersion &&
            persistedA3mInfo.MmseqsVersion != Settings.MmseqsVersion) return false;

        //don't think distinct works, it will probably deserialize to separate objects even if the same
        var dbTargetsInResult = persistedA3mInfo.MsaOriginDefinitions.Select(x => x.SourceDatabaseTarget).Distinct().ToList();

        var a = dbTargetsInResult;

        // if any database target is not fully fitting, the result doesn't fit
        foreach (var dbTarget in MmseqsSourceDatabaseTargets)
        {
            var matchingDbInPersistedResult = dbTargetsInResult.Where(x =>
            {
                var nameFits = x.Database.Name.Equals(dbTarget.Database.Name, StringComparison.OrdinalIgnoreCase);
                var pairingFits = x.UseForPaired == dbTarget.UseForPaired;
                var unpairedFits = x.UseForUnpaired == dbTarget.UseForUnpaired;
                return nameFits && pairingFits && unpairedFits;
            });

            if (!matchingDbInPersistedResult.Any()) return false;
        }

        // if nothing made it *not* work, then it works... Dangerous way if I update stuff, but eh. This is the function that needs to be kept up to date.
        return true;
    }

    private bool IsPersistedMmseqsDatabaseVersionCompatible(string mmseqsHelperDatabaseVersion)
    {
        //TODO: make this settable by strategy
        return mmseqsHelperDatabaseVersion == Settings.ColabfoldMmseqsHelperDatabaseVersion;
    }


    private async Task<List<Protein>> GetProteinTargetsForMonoDbSearchAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> excludeIds)
    {
        var preds = await GetRectifiedTargetPredictionsAsync(inputFastaPaths.ToList(), excludeIds.ToList());
        var proteinsInPreds = preds.SelectMany(x => x.UniqueProteins).Distinct().ToList();
        return proteinsInPreds;

        //var uniqueProteins = new HashSet<Protein>(new ProteinByIdComparer());

        //var excludedList = excludeIds.ToList();

        //foreach (var inputFastaPath in inputFastaPaths)
        //{
        //    if (!File.Exists(inputFastaPath))
        //    {
        //        _logger.LogWarning($"Provided input path does not exist, will skip it: {inputFastaPath}");
        //        continue;
        //    }

        //    await using var stream = File.OpenRead(inputFastaPath);
        //    var fastas = await FastaHelper.GetFastaEntriesIfValidAsync(stream, SequenceType.Protein);
        //    if (fastas is not null && fastas.Any())
        //    {
        //        foreach (var fastaEntry in fastas)
        //        {
        //            var protein = new Protein()
        //                { Id = Helper.GetMd5Hash(fastaEntry.Sequence), Sequence = fastaEntry.Sequence };
        //            if (!excludedList.Contains(protein.Id))
        //            {
        //                uniqueProteins.Add(protein);
        //            }
        //        }
        //    }
        //}

        //return uniqueProteins.ToList();

    }

    //TODO: check versions of monos too, not just final a3ms!
    private async Task<(List<Protein> existing, List<Protein> missing)> GetExistingAndMissingSetsAsync(IEnumerable<Protein> iproteins, IEnumerable<string> existingDatabaseLocations)
    {
        var proteins = new List<Protein>(iproteins);
        var existing = new List<Protein>();

        foreach (var existingDatabasePath in existingDatabaseLocations)
        {
            if (!proteins.Any()) break;

            //TODO: many checks - whether the sequence matches, whether the other stuff apart from qdb exists, ...
            var qdbHeaderDb = Path.Join(existingDatabasePath, Settings.PersistedDbQdbName) +
                              $"{Mmseqs.Settings.Mmseqs2Internal_DbHeaderSuffix}";

            var headers = await Mmseqs.GetAllHeadersInSequenceDbHeaderDbAsync(qdbHeaderDb);
            var contained = proteins.Where(x => headers.Contains(Helper.GetMd5Hash(x.Sequence))).ToList();
            existing.AddRange(contained);
            proteins = proteins.Except(existing).ToList();
        }

        var missing = proteins.Except(existing).ToList();

        return (existing, missing);
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

        if (skippedTargets.Any())
        {
            _logger.LogInformation($"Some inputs ({skippedTargets.Count}) were excluded based on the provided exclusion id list.");
        }

        var targets = rectifiedTargets.Select(x => x.target).ToList();
        
        return targets;
    }
    private async Task<bool> IsValidDbFolder(string path)
    {
        if (!Directory.Exists(path)) return false;
        var files = (await Helper.GetFilesAsync(path));
        if (files.Length < Settings.PersistedDbMinimalNumberOfFilesInMonoDbResult) return false;
        if (!files.Select(Path.GetFileName).Contains(Settings.PersistedMonoDbInfoName)) return false;
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