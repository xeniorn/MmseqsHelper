﻿using FastaHelperLib;
using Microsoft.Extensions.Logging;
using System.Text;

namespace MmseqsHelperLib;

public class ColabfoldMmseqsHelper
{
    // keep this on top
    public const string HardcodedColabfoldMmseqsHelperDatabaseVersion = "0.0.4.230421";
    private readonly ILogger<ColabfoldMmseqsHelper> _logger;

    public ColabfoldMmseqsHelper(ColabfoldMmseqsHelperSettings settings, ILogger<ColabfoldMmseqsHelper> logger)
    {
        _logger = logger;
        Settings = settings;
        Mmseqs = new MmseqsHelper(Settings.ComputingConfig.MmseqsSettings, logger);

        var (refDbTarget, databaseTargets) = GetDatabaseTargets();

        ReferenceSourceDatabaseTarget = refDbTarget;
        MmseqsSourceDatabaseTargets = databaseTargets;

        InstanceInfo.InitalizeFromSettings(Settings);

        InstanceInfo.HelperDatabaseVersion = GetColabfoldMmseqsHelperDatabaseVersion();
        InstanceInfo.MmseqsVersion = GetMmseqsVersion();

        MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs = NonParametrizedMmseqsOptions.Combine(new[]
        {
            MmseqsHelper.GetOptionsThatEnsureMergedResults()
        });
    }

    public NonParametrizedMmseqsOptions? MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs { get;}
    
    public MmseqsHelper Mmseqs { get; }

    public List<MmseqsSourceDatabaseTarget> MmseqsSourceDatabaseTargets { get; }

    public MmseqsSourceDatabaseTarget ReferenceSourceDatabaseTarget { get; }

    public ColabfoldMmseqsHelperSettings Settings { get; set; }

    public async Task GetExistingResultsFromDatabaseAndNameThemAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> persistedA3mPaths, string outputPath)
    {
        var inputPathsList = inputFastaPaths.ToList();
        var persistedA3mPathsList = persistedA3mPaths.ToList();

        // rectify all targets, giving them standard ordering, capitalization, and referencing the same set of Protein instances
        _logger.LogInformation("Getting a list of protein targets from inputs...");
        var rectifiedPredictionTargets = await GetRectifiedTargetPredictionsAsync(inputPathsList);

        var uniqueRectifiedPredictionTargets = rectifiedPredictionTargets.DistinctBy(Helper.GetAutoHashIdWithoutMultiplicity).ToList();
        _logger.LogInformation($"{rectifiedPredictionTargets.Count} targets found ({uniqueRectifiedPredictionTargets.Count} unique).");
        
        if (!rectifiedPredictionTargets.Any()) return;


        //TODO: report on older versions of results too
        _logger.LogInformation($"Checking if any targets have existing predictions...");
        var (existingTargets, missingTargets) = await GetExistingAndMissingPredictionTargetsAsync(uniqueRectifiedPredictionTargets, persistedA3mPathsList);
        
        if (missingTargets.Any())
        {
            _logger.LogInformation($"{existingTargets.Count}/{existingTargets.Count + missingTargets.Count} targets exist, others not available.");
        }
        
        var batchGuid = Guid.NewGuid();
        var batchId = batchGuid.ToString();
        var shortBatchId = Helper.GetMd5Hash(batchId).Substring(0, Settings.PersistedA3mDbConfig.ShortBatchIdLength);

        var fullOutputDir = Path.Join(outputPath, shortBatchId);

        _logger.LogInformation($"Copying files from persisted database to output ({fullOutputDir})...");
        var allSuccessful = await CopyA3mResultsFromDbToTargetAsync( rectifiedPredictionTargets, existingTargets, persistedA3mPathsList, fullOutputDir);

        _logger.LogInformation($"Number of MSA obtained: {allSuccessful.Count}/{rectifiedPredictionTargets.Count}");
    }

    private async Task<List<IProteinPredictionTarget>> CopyA3mResultsFromDbToTargetAsync(List<IProteinPredictionTarget> allTargets, List<IProteinPredictionTarget> existingTargets, List<string> persistedA3mPathsList, string outputDir)
    {
        var uniqueExistingTargets = existingTargets.DistinctBy(Helper.GetAutoHashIdWithoutMultiplicity).ToList();
        var mapping = await GetPredictionTargetToExistingDbPathMappingAsync(uniqueExistingTargets, persistedA3mPathsList);

        var copyTasks = new List<Task>();

        var destination1 = Path.Join(outputDir, "sequential");
        var destination2 = Path.Join(outputDir, "header");

        await Helper.CreateDirectoryAsync(destination1);
        await Helper.CreateDirectoryAsync(destination2);

        var counter = 0;

        var failSet = new HashSet<IProteinPredictionTarget>();

        var idCounts = new Dictionary<string, int>();

        const string emptyName = "invalid_fasta_header";
        const int maxFileBaseLength = 30;

        var copyBatches = GetBatches(allTargets, 100);

        foreach (var batch in copyBatches)
        {
            _logger.LogInformation($"Saving batch {counter}-{counter + batch.Count - 1}...");

            foreach (var target in batch)
            {
                var fileFriendlyIdOrig = Helper.GetFileFriendlyId(target.UserProvidedId);
                var fileFriendlyId =
                    fileFriendlyIdOrig.Substring(0, Math.Min(maxFileBaseLength, fileFriendlyIdOrig.Length));
                var id = string.IsNullOrWhiteSpace(fileFriendlyId) ? emptyName : fileFriendlyId;

                idCounts.TryAdd(id, 0);
                idCounts[id] = idCounts[id] + 1;

                var trueNameBase = idCounts[id] == 1 ? id : $"{id}_{idCounts[id]}";

                var matchedUniqueTarget = uniqueExistingTargets.SingleOrDefault(x =>
                    Helper.GetAutoHashIdWithoutMultiplicity(x) == Helper.GetAutoHashIdWithoutMultiplicity(target));

                if (matchedUniqueTarget is not null)
                {
                    var existingDbPath = mapping.Single(x => x.target == matchedUniqueTarget).dbPath;

                    //TODO: hardcoded, ewww
                    var sourcePath = Path.Join(existingDbPath, Settings.PersistedA3mDbConfig.ResultA3mFilename);
                    var name = counter + ".a3m";
                    var trueName = trueNameBase + ".a3m";

                    var destinationPath = Path.Join(destination1, name);
                    var otherDestinationPath = Path.Join(destination2, trueName);

                    bool fail1, fail2;

                    try
                    {
                        copyTasks.Add(Helper.CopyFileIfExistsAsync(sourcePath, destinationPath));
                        fail1 = false;
                    }
                    catch (Exception ex)
                    {
                        fail1 = true;
                    }

                    try
                    {
                        copyTasks.Add(Helper.CopyFileIfExistsAsync(sourcePath, otherDestinationPath));
                        fail2 = false;
                    }
                    catch (Exception ex)
                    {
                        fail2 = true;
                    }

                    if (fail1 && fail2) failSet.Add(target);
                }

                counter++;
            }

            await Task.WhenAll(copyTasks);
            copyTasks.Clear();

        }

        var successful = allTargets.Except(failSet).ToList();
        return successful;
    }

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
        _logger.LogInformation("Getting a list of protein targets from inputs...");
        var rectifiedPredictionTargets = await GetRectifiedTargetPredictionsAsync(inputPathsList);
        var uniqueRectifiedPredictionTargets = rectifiedPredictionTargets.DistinctBy(Helper.GetAutoHashIdWithoutMultiplicity).ToList();
        _logger.LogInformation($"{rectifiedPredictionTargets.Count} targets found ({uniqueRectifiedPredictionTargets.Count} unique).");

        var skippedTargets = uniqueRectifiedPredictionTargets.Where(x => excludedHashList.Contains(Helper.GetAutoHashIdWithoutMultiplicity(x))).ToList();
        var nonSkippedUniqueTargets = uniqueRectifiedPredictionTargets.Except(skippedTargets).ToList();

        if (skippedTargets.Any())
        {
            _logger.LogInformation($"Some unique inputs ({skippedTargets.Count}) were excluded based on the provided exclusion id list.");
        }


        _logger.LogInformation($"Checking if any targets have existing predictions...");
        var (existingTargets, missingTargets) = await GetExistingAndMissingPredictionTargetsAsync(nonSkippedUniqueTargets, persistedA3mPathsList);

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
            _logger.LogInformation($"Number of missing predictions containing {Helper.GetMultimerName(numberOfMonomers)}s: {targetList.Count}");
        }

        _logger.LogInformation($"Checking for presence of precalculated mono dbs required for final MSA assembly...");
        // all predictions use same mono references (are "rectified"), distinct by reference is ok
        var allMonos = missingTargets.SelectMany(x => x.UniqueProteins).Distinct().ToList();
        var (existingMonos, missingMonos) = await GetExistingAndMissingProteinsAsync(allMonos, filteredPersistedMonoDbPaths);

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

        _logger.LogInformation($"Will attempt to generate MSA results for ({predictableTargets.Count}/{missingTargets.Count}) targets.");

        var allSuccessful = new List<IProteinPredictionTarget>();

        var batches = GetBatches(predictableTargets, Settings.ComputingConfig.MaxDesiredPredictionTargetBatchSize);
        _logger.LogInformation($"Inputs split into {batches.Count} batches.");
        foreach (var targetBatch in batches)
        {
            try
            {
                var successfulList =
                    await GenerateA3msFromFastasGivenExistingMonoDbsAsync(outputPath, filteredPersistedMonoDbPaths,
                        targetBatch);
                allSuccessful.AddRange(successfulList);
            }
            catch (Exception ex)
            {
                var policy = Settings.Strategy.ProcessingBatchFailedCatastrophically;
                var message = $"Exception thrown while processing batch.";
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.Report))
                {
                    _logger.LogError(message);
                }
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.SkipCurrentItem))
                {
                    continue;
                }
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.StopProcessing))
                {
                    break;
                }
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.KillProgram))
                {
                    throw new Exception(message);
                }
            }
        }

        _logger.LogInformation($"Number of MSA predictions carried out: {allSuccessful.Count}/{predictableTargets.Count}");

    }

    public async Task GenerateColabfoldMonoDbsFromFastasAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> persistedMonoDatabaseParentFolderLocations, IEnumerable<string> excludedIds, string outputPath)
    {
        var excludedIdList = excludedIds.ToList();
        var persistedMonoDbPaths = await GetDbEntryFoldersAsync(persistedMonoDatabaseParentFolderLocations.ToList());
        
        var uniqueTargets = await GetProteinTargetsForMonoDbSearchAsync(inputFastaPaths, excludedIdList);
        var requiredFeatures = GetRequiredMonoDbFeaturesForTargets(uniqueTargets);

        int existingDbParallelSearchBatchSize = Settings.ComputingConfig.ExistingDatabaseSearchParallelizationFactor;
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

        var batches = GetBatches<Protein>(missingTargets, Settings.ComputingConfig.MaxDesiredMonoBatchSize);

        foreach (var proteinBatch in batches)
        {
            await GenerateColabfoldMonoDbsForProteinBatchAsync(outputPath, proteinBatch);
        }
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
                    persistedDbName = Settings.PersistedMonoDbConfig.ForPairingAlignDbName;
                    break;
                case ColabfoldMsaDataType.Unpaired:
                    persistedDbName = Settings.PersistedMonoDbConfig.MonoA3mDbName;
                    break;
                default: throw new NotImplementedException("This shouldn't ever happen.");
            }

            // there should be only one matching subfolder? we are looking at features that have the same location, db name, type. So they should map to the same subfolder
            // still this is fragile, therefore TODO: make this better
            var dbTargetSubfolder = featuresInThisPersistedDb.Select(x => x.FeatureSubFolderPath).Distinct().Single();
            string mmseqsDb = Path.Join(dbLocation, dbTargetSubfolder, persistedDbName);
            // queue up the reads
            resultTasksMapping.Add((dbLocation, Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(mmseqsDb, mmseqsIndicesForProteinsInThisPersistedDb)));
        }

        // proceed after all the batched reads are done
        var tasks = resultTasksMapping.Select(x => x.resultTask).ToList();
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            var exceptions = tasks.Where(x => x.Exception is not null).ToList();
            if (exceptions.Any())
            {
                _logger.LogWarning($"Reading entries from some locations ({exceptions.Count}/{tasks.Count}) failed unexpectedly.");
            }
        }

        // now, combine all of these into the actual dict we want, where it's mapped to proteins, not mmseqs db indices like before
        foreach (var (dbLocation, resultTask) in resultTasksMapping.Where(x=>x.resultTask.IsCompletedSuccessfully))
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

            // fire and forget
            if (Settings.ComputingConfig.ReportSuccessfulUsageOfPersistedDb) _ = Task.Run(() => ReportMonoDbWasUsedAsync(dbLocation)).ConfigureAwait(false);
        }
    }

    private async Task ReportMonoDbWasUsedAsync(string monoDbPath)
    {
        var reporterFile = Path.Join(monoDbPath, Settings.PersistedMonoDbConfig.LastAccessReporterFilename);
        await Task.Run(() => Helper.Touch(reporterFile));
    }

    private async IAsyncEnumerable<ColabFoldMsaObject> AutoCreateColabfoldMsaObjectsAsync(List<IProteinPredictionTarget> predictions, MmseqsDbLocator locator)
    {
        var targetsWithPairing = GetPredictionsThatRequirePairing(predictions);

        foreach (var predictionTarget in predictions)
        {
            ColabFoldMsaObject msaObj;
            try
            {
                List<AnnotatedMsaData> dataEntries = new List<AnnotatedMsaData>();

                var qdbIndices = locator.QdbIndicesMapping[predictionTarget];

                foreach (var dbTarget in MmseqsSourceDatabaseTargets)
                {
                    var pairedDataCollection = dbTarget.UseForPaired
                        ? await Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(locator.PairedA3mDbPathMapping[dbTarget],
                            qdbIndices)
                        : null;
                    var unpairedDataCollection = dbTarget.UseForUnpaired
                        ? await Mmseqs.ReadEntriesWithIndicesFromDataDbAsync(locator.UnPairedA3mDbPathMapping[dbTarget],
                            qdbIndices)
                        : null;

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

                msaObj = new ColabFoldMsaObject(dataEntries, predictionTarget);
            }
            catch (Exception ex)
            {
                var policy = Settings.Strategy.FinalMsaGenerationFailedForSinglePrediction;
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.Report))
                {
                    _logger.LogError($"Error encountered while generating the MSA object for prediction target {predictionTarget.AutoId}.");
                }
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.SkipCurrentItem))
                {
                    _logger.LogDebug("Skipping failed target.");
                    continue;
                }
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.StopProcessing))
                {
                    _logger.LogDebug("Stopping processing.");
                    break;
                }
                if (policy.ActionsRequsted.Contains(IssueHandlingAction.KillProgram))
                {
                    throw;
                }
                // default action
                throw;
            }
            yield return msaObj;
        }

    }

    private async Task<string> AutoMergeMonoDbsAsync(string workingDir, string qdb, string uniprotMonoDb, string envDbMonoDb)
    {
        var localProcessingPath = Path.Join(workingDir, "final");
        await Helper.CreateDirectoryAsync(localProcessingPath);

        //*******************************************merge the mono dbs*******************************************************
        var mergeMonoResultDb = Path.Join(localProcessingPath, Settings.PersistedMonoDbConfig.MonoA3mDbName);
        var (success, resultDbPath) = await Mmseqs.RunMergeAsync(qdb, mergeMonoResultDb, new List<string> { uniprotMonoDb, envDbMonoDb }, String.Empty);

        if (!success)
        {
            _logger.LogError("Merge failed for some reason.");
            throw new Exception();
        }

        return resultDbPath;

    }

    private async Task<bool> CheckIsAcceptablePersistedA3mResultForTargetAsync(IProteinPredictionTarget target, string infoFilePath)
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

        if (!IsMmseqsHelperDatabaseVersionCompatible(persistedA3mInfo.MmseqsHelperDatabaseVersion)) return false;

        // just exploring a bit how structured issue policy implementation might look like... Seems to take a lot of work.
        if (persistedA3mInfo.MmseqsVersion != InstanceInfo.MmseqsVersion)
        {
            var message = $"Version of Mmseqs in persisted db ({persistedA3mInfo.MmseqsVersion}) and current ({InstanceInfo.MmseqsVersion}) do not match.";
            var policy = Settings.Strategy.MmseqsVersionOfPersistedDatabaseIsLowerThanRunningVersion;
            if (policy.ActionsRequsted.Contains(IssueHandlingAction.Report))
            {
                var logLevel = policy.GetValueOrDefault<LogLevel>("LogLevel", LogLevel.Warning);
                _logger.Log(logLevel, message);
            }
            if (policy.ActionsRequsted.Contains(IssueHandlingAction.SkipCurrentItem))
            {
                _logger.LogInformation("Will not use it.");
                return false;
            }
            if (policy.ActionsRequsted.Contains(IssueHandlingAction.StopProcessing) || policy.ActionsRequsted.Contains(IssueHandlingAction.KillProgram))
            {
                throw new Exception(message);
            }
        }

        //don't think distinct works, it will probably deserialize to separate objects even if the same
        var dbTargetsInResult = persistedA3mInfo.MsaOriginDefinitions.Select(x => x.SourceDatabaseTarget).Distinct().ToList();
        
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
    private async Task<(MmseqsDbLocator locator, List<IProteinPredictionTarget> feasiblePredictions)> CompileSourceMonoDbsFromPersistedAsync(
        string workingDir, List<string> persistedMonoDatabasesPaths, List<IProteinPredictionTarget> predictionBatch)
    {
        var dbProcessingBatchSize = Settings.ComputingConfig.ExistingDatabaseSearchParallelizationFactor;

        await Helper.CreateDirectoryAsync(workingDir);

        var locator = new MmseqsDbLocator();

        //*******************************************figure out which mono dbs contain relevant entries at which indices*******************************************************
        //******************************************* check which persisted dbs used each target mono and have the desired features *******************************************
        var requiredFeatures = GetRequiredMonoDbFeaturesForPredictions(predictionBatch);
        var (missingFeatures, usableFeatures) =
            await GetUsableAndMissingFeaturesAsync(persistedMonoDatabasesPaths, requiredFeatures, dbProcessingBatchSize);

        var unfeasibleMonos = missingFeatures.Select(x => x.Mono).ToList();
        List<IProteinPredictionTarget> unfeasiblePredictions = new List<IProteinPredictionTarget>();
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
                return (locator, new List<IProteinPredictionTarget>());
            }
        }
        
        List<Task> writeTasks = new List<Task>();

        var feasiblePredictions = predictionBatch.Except(unfeasiblePredictions).ToList();

        var predictionToIndicesMapping = GeneratePredictionToIndexMapping(feasiblePredictions);
        locator.QdbIndicesMapping = predictionToIndicesMapping;

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
                .Where(x => x.DatabaseName == dbTarget.Database.Name).ToList();

            var localPath = Path.Join(workingDir, dbTarget.Database.Name);
            await Helper.CreateDirectoryAsync(localPath);

            var monoToAlignFragmentMappings = dbTarget.UseForPaired ? await GenerateProteinToMonoDataFragmentMappings(
                featuresForThisSourceDb, dbTarget, feasiblePredictions, unfeasiblePredictions,
                ColabfoldMsaDataType.Paired) : new Dictionary<Protein, byte[]>();

            var monoToUnpairedA3mFragmentMappings = dbTarget.UseForUnpaired ? await GenerateProteinToMonoDataFragmentMappings(
                featuresForThisSourceDb, dbTarget, feasiblePredictions, unfeasiblePredictions,
                ColabfoldMsaDataType.Unpaired) : new Dictionary<Protein, byte[]>();

            unfeasiblePredictions.ForEach(x=> locator.QdbIndicesMapping.Remove(x));
            if (!feasiblePredictions.Any())
            {
                return (locator, new List<IProteinPredictionTarget>());
            }

            if (dbTarget.UseForPaired)
            {
                var dataType = ColabfoldMsaDataType.Paired;
                var alignDbObject = GenerateDbObjectForPredictionBatch(feasiblePredictions, monoToAlignFragmentMappings, locator, dataType);

                var pairedAlignDb = Path.Join(localPath, "align1");
                var alignDbDataDbPath = $"{pairedAlignDb}{Mmseqs.Settings.Mmseqs2Internal.DbDataSuffix}";

                writeTasks.Add(alignDbObject.WriteToFileSystemAsync(Mmseqs.Settings, alignDbDataDbPath));
                locator.PrePairingAlignDbPathMapping.Add(dbTarget, pairedAlignDb);
            }

            if (dbTarget.UseForUnpaired)
            {
                var dataType = ColabfoldMsaDataType.Unpaired;
                var unpairedA3mDbObject = GenerateDbObjectForPredictionBatch(feasiblePredictions, monoToUnpairedA3mFragmentMappings, locator, dataType);

                var unpairedA3mDb = Path.Join(localPath, "unpaired_a3m");
                var unpairedA3mDbDataDbPath = $"{unpairedA3mDb}{Mmseqs.Settings.Mmseqs2Internal.DbDataSuffix}";

                writeTasks.Add(unpairedA3mDbObject.WriteToFileSystemAsync(Mmseqs.Settings, unpairedA3mDbDataDbPath));
                locator.UnPairedA3mDbPathMapping.Add(dbTarget, unpairedA3mDb);
            }
        }
        
        //******************************************* generate new qdb *******************************************************
        var queryDatabaseForPairing = GenerateQdbForPairing(feasiblePredictions, predictionToIndicesMapping);

        var pairedQdb = Path.Join(workingDir, "qdb_pair");
        locator.PairingQdbPath = pairedQdb;

        writeTasks.Add(queryDatabaseForPairing.WriteToFileSystemAsync(Mmseqs.Settings, pairedQdb));
        
        await Task.WhenAll(writeTasks);
        return (locator, feasiblePredictions);
    }

    private async Task<Dictionary<Protein, byte[]>> GenerateProteinToMonoDataFragmentMappings(
        List<MmseqsPersistedMonoDbEntryFeature> featuresForThisSourceDb, MmseqsSourceDatabaseTarget dbTarget,
        List<IProteinPredictionTarget> mutableFeasiblePredictions, List<IProteinPredictionTarget> mutableUnfeasiblePredictions, ColabfoldMsaDataType dataType)
    {
        var dbProcessingBatchSize = Settings.ComputingConfig.ExistingDatabaseSearchParallelizationFactor;

        Dictionary<Protein, byte[]> monoToFragmentMappings = new();
        
        var relevantFeatures = featuresForThisSourceDb
            .Where(x => x.SourceType == dataType).ToList();

        if (!relevantFeatures.Any()) return monoToFragmentMappings;

        monoToFragmentMappings = await TryGetDataDbFragmentsForSourceDatabaseAsync(
            dbTarget.Database, relevantFeatures, dataType, dbProcessingBatchSize);

        var successfulMonos = monoToFragmentMappings.Select(x => x.Key).ToList();
        var unsuccessfulMonos = relevantFeatures.Select(x => x.Mono).Except(successfulMonos).ToList();

        if (unsuccessfulMonos.Any())
        {
            _logger.LogWarning(
                $"Failed obtaining the data for {unsuccessfulMonos.Count}/{relevantFeatures.Count} required features/monos.");

            var unfeasibleDueToThisDataType = mutableFeasiblePredictions
                .Where(x => x.UniqueProteins.Any(prot => unsuccessfulMonos.Contains(prot))).ToList();
            _logger.LogInformation($"Predictions made unfeasible: {unfeasibleDueToThisDataType.Count}");

            mutableFeasiblePredictions.RemoveAll(unfeasibleDueToThisDataType.Contains);
            mutableUnfeasiblePredictions.AddRange(unfeasibleDueToThisDataType);
            // remove features regardless of pairing; since we can't produce the result unless both were successful
            featuresForThisSourceDb.RemoveAll(x => unsuccessfulMonos.Contains(x.Mono));

            _logger.LogInformation($"Feasible predictions remaining in batch: {mutableFeasiblePredictions.Count}");
        }

        return monoToFragmentMappings;
    }

    private async Task<List<IProteinPredictionTarget>> GenerateA3msFromFastasGivenExistingMonoDbsAsync(string outputPath, List<string> realPersistedMonoDatabasesPaths, List<IProteinPredictionTarget> predictionBatch)
    {
        var resultList = new List<IProteinPredictionTarget>();

        var batchGuid = Guid.NewGuid();
        var batchId = batchGuid.ToString();
        var shortBatchId = Helper.GetMd5Hash(batchId).Substring(0, Settings.PersistedA3mDbConfig.ShortBatchIdLength);

        var workingDir = Path.Join(Settings.ComputingConfig.TempPath, batchId);
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
            return new List<IProteinPredictionTarget>();
        }
        if (feasiblePredictionTargets.Count != predictionBatch.Count)
        {
            // var diff = predictionBatch.Count - feasiblePredictionTargets.Count;
            _logger.LogWarning($"Some targets were unfeasible, continuing with ({feasiblePredictionTargets.Count}/${predictionBatch.Count}");
        }

        //*******************************************perform pairing*******************************************************
        if (MmseqsSourceDatabaseTargets.Any(x=>x.UseForPaired)) LogSomething($"Performing MSA pairing...");

        //TODO: for now I anyhow have only uniref for pairing, but this can be parallelized for all paired dbs, do this parallelization!
        foreach (var dbTarget in MmseqsSourceDatabaseTargets.Where(x => x.UseForPaired))
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
            writeTasks.Add(msaObject.WriteToFileSystemAsync(Settings, fullResultPath, InstanceInfo));
            resultList.Add(msaObject.PredictionTarget);
        }

        await Task.WhenAll(writeTasks);
        LogSomething($"Wrote {msaObjectCounter} result files in {outputPath}.");

        if (Settings.ComputingConfig.DeleteTemporaryData)
        {
            var cleanupTasks = new List<Task>();
            _logger.LogInformation($"Cleaning up temporary location ({workingDir}) ...");
            cleanupTasks.Add(Task.Run(() => Helper.RecursiveDeleteDirectoryAsync(workingDir)));
            await Task.WhenAll(cleanupTasks);
            _logger.LogInformation($"Done cleaning up temporary location.");
        }
        else
        {
            _logger.LogInformation($"Temp data not deleted as requested ({workingDir}).");
        }

        return resultList;
    }

    public ColabfoldHelperComputationInstanceInfo InstanceInfo { get; set; } = new ();

    private async Task<string> GenerateAlignDbForPairingAsync(string workingDir, string qdbPath, string searchResultDb,
        MmseqsSourceDatabaseTarget dbTarget)
    {
        var localProcessingPath = Path.Join(workingDir, dbTarget.Database.Name, "pair");
        await Helper.CreateDirectoryAsync(localProcessingPath);
        
        var targetDbPathSeq = Mmseqs.GetProperDatabaseSeqPath(dbTarget.Database.Path);
        var targetDbPathAln = Mmseqs.GetProperDatabaseAlignPath(dbTarget.Database.Path);

        var performanceParams = Mmseqs.PerformanceParams(dbTarget.RequestPreloadingToRam);
        

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.ColabfoldMmseqsParams.Paired.Expand} {performanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, Settings.PersistedMonoDbConfig.ForPairingAlignDbName);
        var alignPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,

        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.ColabfoldMmseqsParams.Paired.Align1} {performanceParams}",
            MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs);

        return alignResultDb;
    }

    private async Task GenerateColabfoldMonoDbsForProteinBatchAsync(string outputBasePath, List<Protein> proteinBatch)
    {
        var batchId = Guid.NewGuid().ToString();

        var workingDir = Path.Join(Settings.ComputingConfig.TempPath, batchId);
        var outDir = Path.Join(outputBasePath, batchId);

        await Helper.CreateDirectoryAsync(workingDir);
        await Helper.CreateDirectoryAsync(outDir);

        LogSomething($"starting batch {batchId} with {proteinBatch.Count} items");

        //*******************************************create source fasta file*******************************************************
        var qdbWorkingDir = Path.Join(workingDir, "qdb");
        await Helper.CreateDirectoryAsync(qdbWorkingDir);
        var fastaName = $"input.fasta";
        var queryFastaPath = Path.Join(qdbWorkingDir, fastaName);
        using (var fastaFileOutputStream = File.Create(queryFastaPath))
        {
            await foreach (var fastaChunk in FastaHelper.GenerateMultiFastaDataAsync(proteinBatch))
            {
                await fastaFileOutputStream.WriteAsync(Encoding.ASCII.GetBytes(fastaChunk));
            }
        }

        //*******************************************create query db file*******************************************************
        var qdbNameBase = $"{Settings.PersistedMonoDbConfig.QdbName}";
        var qdbPath = Path.Join(qdbWorkingDir, qdbNameBase);

        var createDbParameters = new CreateDbParameters();
        createDbParameters.ApplyDefaults();
        await Mmseqs.CreateDbAsync(new [] {queryFastaPath}, qdbPath, createDbParameters);

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
        foreach (var dbTarget in MmseqsSourceDatabaseTargets.Where(x => x != ReferenceSourceDatabaseTarget))
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

        var finalPathQdb = Path.Join(outDir, Settings.PersistedMonoDbConfig.QdbName);
        copyTasks.Add(Mmseqs.CopyDatabaseAsync(qdbPath, finalPathQdb));
        _logger.LogInformation($"Query db output: {finalPathQdb}");

        var finalPathProfile = Path.Join(outDir, ReferenceSourceDatabaseTarget.Database.Name, Settings.PersistedMonoDbConfig.SearchProfileDbName);
        copyTasks.Add(Mmseqs.CopyDatabaseAsync(refProfileDb, finalPathProfile));
        _logger.LogInformation($"Database {ReferenceSourceDatabaseTarget.Database.Name} search profile output: {finalPathProfile}");

        if (ReferenceSourceDatabaseTarget.UseForUnpaired)
        {
            var monoDbPath = refProcessingTasks.Single(x => x.msaType == ColabfoldMsaDataType.Unpaired).task.Result;
            var finalPathMonos = Path.Join(outDir, ReferenceSourceDatabaseTarget.Database.Name, Settings.PersistedMonoDbConfig.MonoA3mDbName);
            copyTasks.Add(Mmseqs.CopyDatabaseAsync(monoDbPath, finalPathMonos));
            _logger.LogInformation($"Database {ReferenceSourceDatabaseTarget.Database.Name} unpair output: {finalPathMonos}");
        }
        if (ReferenceSourceDatabaseTarget.UseForPaired)
        {
            var alignDbPath = refProcessingTasks.Single(x => x.msaType == ColabfoldMsaDataType.Paired).task.Result;
            var finalPathAlign = Path.Join(outDir, ReferenceSourceDatabaseTarget.Database.Name, Settings.PersistedMonoDbConfig.ForPairingAlignDbName);
            copyTasks.Add(Mmseqs.CopyDatabaseAsync(alignDbPath, finalPathAlign));
            _logger.LogInformation($"Database {ReferenceSourceDatabaseTarget.Database.Name} align for pair output: {finalPathAlign}");
        }

        foreach (var dbTarget in MmseqsSourceDatabaseTargets.Where(x => x != ReferenceSourceDatabaseTarget))
        {
            var paths = otherDbProcessingTasks.Single(x => x.dbTarget == dbTarget).task.Result;
            if (dbTarget.UseForUnpaired)
            {
                var monoDbPath = paths.unpairedDb;
                var finalPathMonos = Path.Join(outDir, dbTarget.Database.Name, Settings.PersistedMonoDbConfig.MonoA3mDbName);
                copyTasks.Add(Mmseqs.CopyDatabaseAsync(monoDbPath, finalPathMonos));
                _logger.LogInformation($"Database {dbTarget.Database.Name} unpair output: {finalPathMonos}");
            }
            if (dbTarget.UseForPaired)
            {
                var alignDbPath = paths.pairedDb;
                var finalPathAlign = Path.Join(outDir, dbTarget.Database.Name, Settings.PersistedMonoDbConfig.ForPairingAlignDbName);
                copyTasks.Add(Mmseqs.CopyDatabaseAsync(alignDbPath, finalPathAlign));
                _logger.LogInformation($"Database {dbTarget.Database.Name} align for pair output: {finalPathAlign}");
            }
        }

        await Task.WhenAll(copyTasks);
        
        //******************************************* print out the database info *************************************
        //TODO: refactor / encapsulate this
        var info = new PersistedMonoDbMetadataInfo(createTime: DateTime.Now,
            referenceDbTarget: ReferenceSourceDatabaseTarget, databaseTargets: MmseqsSourceDatabaseTargets,
            mmseqsHelperDatabaseVersion: InstanceInfo.HelperDatabaseVersion, targetCount: proteinBatch.Count,
            mmseqsVersion: InstanceInfo.MmseqsVersion, computationInfoReport:new ComputationInfoReport(Settings, InstanceInfo));
        info.LoadLengths(proteinBatch);

        var infoPath = Path.Join(outDir, Settings.PersistedMonoDbConfig.InfoFilename);
        await info.WriteToFileSystemAsync(infoPath);
        _logger.LogInformation($"Database info file: {infoPath}");

        //******************************************* delete temp *************************************
        
        if (Settings.ComputingConfig.DeleteTemporaryData)
        {
            var cleanupTasks = new List<Task>();
            _logger.LogInformation($"Cleaning up temporary location ({workingDir}) ...");
            cleanupTasks.Add(Task.Run(() => Helper.RecursiveDeleteDirectoryAsync(workingDir)));
            await Task.WhenAll(cleanupTasks);
            _logger.LogInformation($"Done cleaning up temporary location.");
        }
        else
        {
            _logger.LogInformation($"Temp data not deleted as requested ({workingDir}).");
        }

    }
    

    /// <summary>
    /// Contains original data for each protein target, doesn't strip anything except the data terminator \0
    /// Is not guaranteed it will return an entry for all inputs!
    /// </summary>
    /// <param name="targetDb"></param>
    /// <param name="featuresCollection"></param>
    /// <param name="dataType"></param>
    /// <param name="persistedDbLocationBatchSize"></param>
    /// <returns></returns>
    private async Task<Dictionary<Protein, byte[]>> TryGetDataDbFragmentsForSourceDatabaseAsync(
        MmseqsSourceDatabase targetDb,
        List<MmseqsPersistedMonoDbEntryFeature> featuresCollection,
        ColabfoldMsaDataType dataType,
        int persistedDbLocationBatchSize)
    {
        // actual data of each aligndb for each mono
        var monoToDataFragmentMappings = new Dictionary<Protein, byte[]>();

        var relevantFeatures = featuresCollection
            .Where(x => x.DatabaseName == targetDb.Name && x.SourceType == dataType).ToList();
        var relevantLocations = relevantFeatures
            .Select(x => x.DbPath).Distinct().ToList();

        var relevantLocationBatches = GetBatches(relevantLocations, persistedDbLocationBatchSize);
        _logger.LogDebug($"Search locations pooled into {relevantLocationBatches.Count} batches.");
        foreach (var searchBatch in relevantLocationBatches)
        {
            _logger.LogDebug($"Searching a batch with {searchBatch.Count} locations...");
            // the monoToAlignFragmentMappings dict gets mutated each round
            await AppendFragmentDictionaryForSearchBatchAsync(monoToDataFragmentMappings, searchBatch, relevantFeatures, dataType);
        }

        return monoToDataFragmentMappings;
    }

    private MmseqsDatabaseObject GenerateDbObjectForPredictionBatch(
        List<IProteinPredictionTarget> predictionBatch, Dictionary<Protein, byte[]> monoToDataFragmentMappings, MmseqsDbLocator locator, ColabfoldMsaDataType databaseType)
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

            for (int i = 0; i < indices.Count; i++)
            {
                var protein = predictionTarget.UniqueProteins[i];
                var monoIndex = indices[i];

                var data = monoToDataFragmentMappings[protein];
                mmseqsDbObject.AddData(data, monoIndex);
            }
        }

        return mmseqsDbObject;
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

    private async Task<string> GenerateMonoA3mDbAsync(string workingDir, string qdbPath, string profileDbPath, string searchDbPath, MmseqsSourceDatabaseTarget dbTarget)
    {
        var localProcessingPath = Path.Join(workingDir, dbTarget.Database.Name, "mono");
        await Helper.CreateDirectoryAsync(localProcessingPath);
        
        var targetDbPathSeq = Mmseqs.GetProperDatabaseSeqPath(dbTarget.Database.Path);
        var targetDbPathAln = Mmseqs.GetProperDatabaseAlignPath(dbTarget.Database.Path);

        var performanceParams = Mmseqs.PerformanceParams(dbTarget.RequestPreloadingToRam);

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.ColabfoldMmseqsParams.Unpaired.Expand} {performanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, $"align");
        var alignPosParams = new List<string>()
        {
            profileDbPath,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.ColabfoldMmseqsParams.Unpaired.Align} {performanceParams}");

        //*******************************************filter*******************************************************
        var filterResultDb = Path.Join(localProcessingPath, $"filter");
        var filterPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            alignResultDb,
            filterResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.filterModule, filterPosParams, $"{Settings.ColabfoldMmseqsParams.Unpaired.Filter} {performanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"mono_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            filterResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.ColabfoldMmseqsParams.Unpaired.MsaConvert} {performanceParams}",
            MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs);

        return msaConvertResultDb;
    }

    private Dictionary<IProteinPredictionTarget, List<int>> GeneratePredictionToIndexMapping(List<IProteinPredictionTarget> targets)
    {
        var predictionTargetToDbIndicesMapping = new Dictionary<IProteinPredictionTarget, List<int>>();
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

    private MmseqsQueryDatabaseContainer GenerateQdbForPairing(List<IProteinPredictionTarget> targets, Dictionary<IProteinPredictionTarget, List<int>> targetToDbIndicesMapping)
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

                qdbDataDbObject.AddData(Encoding.ASCII.GetBytes(protein.Sequence + Mmseqs.Settings.Mmseqs2Internal.SequenceEntryHardcodedSuffix), monoIndex);
                qdbHeaderDbObject.AddData(Encoding.ASCII.GetBytes(protein.Id + Mmseqs.Settings.Mmseqs2Internal.HeaderEntryHardcodedSuffix), monoIndex);
                qdbLookupObject.Add(monoIndex, protein.Id, generatedPredictionIndex);
            }

            generatedPredictionIndex++;
        }

        mmseqsQueryDatabase.DataDbObject = qdbDataDbObject;
        mmseqsQueryDatabase.HeaderDbObject = qdbHeaderDbObject;
        mmseqsQueryDatabase.LookupObject = qdbLookupObject;

        return mmseqsQueryDatabase;
    }

    private async Task<(string searchDb, string profileDb)> GenerateReferenceSearchAndCreateProfileDbAsync(MmseqsSourceDatabaseTarget dbTarget, string workingDir, string qdbPath)
    {
        var processingFolderRoot = Path.Join(workingDir, dbTarget.Database.Name, "search");
        await Helper.CreateDirectoryAsync(processingFolderRoot);
        var searchSubfolder = Path.Join(processingFolderRoot, "tmp");
        await Helper.CreateDirectoryAsync(searchSubfolder);
        var expectedGeneratedProfileSubPath = Path.Join("latest", "profile_1");

        var performanceParams = Mmseqs.PerformanceParams(dbTarget.RequestPreloadingToRam);
        var targetDbPathRoot = Mmseqs.GetProperDatabaseRootPath(dbTarget.Database.Path);

        var searchResultDb = Path.Join(processingFolderRoot, $"search");
        var searchPosParams = new List<string>() { qdbPath, targetDbPathRoot, searchResultDb, searchSubfolder };
        await Mmseqs.RunMmseqsAsync(Mmseqs.searchModule, searchPosParams, $"{Settings.ColabfoldMmseqsParams.Search} {performanceParams}");

        //*******************************************hack up a profile db*******************************************************
        var profileResultDbOriginal = Path.Join(searchSubfolder, expectedGeneratedProfileSubPath);
        var profileResultDb = Path.Join(processingFolderRoot, "profile");

        //***move temp file from search as profile db***
        var movePosParams = new List<string>() { profileResultDbOriginal, profileResultDb };
        await Mmseqs.RunMmseqsAsync(Mmseqs.moveModule, movePosParams, String.Empty);

        //***link to header db of qdb since it has the same values***
        var (success, path) = await Mmseqs.RunLinkDbAsync(qdbPath + Mmseqs.Settings.Mmseqs2Internal.DbHeaderSuffix,
            profileResultDb + Mmseqs.Settings.Mmseqs2Internal.DbHeaderSuffix);

        if (!success)
        {
            throw new Exception("Failed to link the profile db");
        }

        return (searchResultDb, profileResultDb);

    }

    private async Task<string> GenerateSearchDbAsync(MmseqsSourceDatabaseTarget dbTarget,
        string workingDir, string profileDbPath)
    {
        //*******************************************search*******************************************************
        var processingFolderRoot = Path.Join(workingDir, dbTarget.Database.Name, "search");
        await Helper.CreateDirectoryAsync(processingFolderRoot);
        var searchSubfolder = Path.Join(processingFolderRoot, "tmp");
        await Helper.CreateDirectoryAsync(searchSubfolder);

        var performanceParams = Mmseqs.PerformanceParams(dbTarget.RequestPreloadingToRam);
        var targetDbPathRoot = Mmseqs.GetProperDatabaseRootPath(dbTarget.Database.Path);

        var searchResultDb = Path.Join(processingFolderRoot, $"search");
        var searchPosParams = new List<string>() { profileDbPath, targetDbPathRoot, searchResultDb, searchSubfolder };
        await Mmseqs.RunMmseqsAsync(Mmseqs.searchModule, searchPosParams, $"{Settings.ColabfoldMmseqsParams.Search} {performanceParams}",
            MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs);

        return searchResultDb;
    }

    private async Task<string> GenerateSpecialMonoA3mDbForReferenceDbAsync(string workingDir, string qdbPath, string profileResultDb, string searchResultDb, MmseqsSourceDatabaseTarget refDbTarget)
    {
        var localProcessingPath = Path.Join(workingDir, refDbTarget.Database.Name, "mono");
        await Helper.CreateDirectoryAsync(localProcessingPath);
        
        var targetDbPathSeq = Mmseqs.GetProperDatabaseSeqPath(refDbTarget.Database.Path);
        var targetDbPathAln = Mmseqs.GetProperDatabaseAlignPath(refDbTarget.Database.Path);

        var performanceParams = Mmseqs.PerformanceParams(refDbTarget.RequestPreloadingToRam);

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.expandModule, expandPosParams, $"{Settings.ColabfoldMmseqsParamsUnpairedSpecialForReferenceDb.Expand} {performanceParams}");

        //*******************************************align*******************************************************
        var alignResultDb = Path.Join(localProcessingPath, $"align");
        var alignPosParams = new List<string>()
        {
            profileResultDb,
            targetDbPathSeq,
            expandResultDb,
            alignResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, alignPosParams, $"{Settings.ColabfoldMmseqsParamsUnpairedSpecialForReferenceDb.Align} {performanceParams}");

        //*******************************************filter*******************************************************
        var filterResultDb = Path.Join(localProcessingPath, $"filter");
        var filterPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            alignResultDb,
            filterResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.filterModule, filterPosParams, $"{Settings.ColabfoldMmseqsParamsUnpairedSpecialForReferenceDb.Filter} {performanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"mono_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            filterResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.ColabfoldMmseqsParamsUnpairedSpecialForReferenceDb.MsaConvert} {performanceParams}",
            MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs);

        return msaConvertResultDb;
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

    private string GetColabfoldMmseqsHelperDatabaseVersion()
    {
        return HardcodedColabfoldMmseqsHelperDatabaseVersion;

    }

    private (MmseqsSourceDatabaseTarget refDbTarget, List<MmseqsSourceDatabaseTarget> dbTargets) GetDatabaseTargets()
    {
        var refDbTarget = Settings.SearchDatabasesConfig.ReferenceDbTarget;
        var dbTargets = Settings.SearchDatabasesConfig.DbTargets;
        return (refDbTarget, dbTargets);

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
            var foldersWithTask = foldersInside.Select(x => (folder: x, checkValidityTask: IsValidDbFolder(x))).ToList();
            await Task.WhenAll(foldersWithTask.Select(x => x.checkValidityTask));
            var validFolders = foldersWithTask.Where(x => x.checkValidityTask.Result).Select(x => x.folder);
            result.AddRange(validFolders);
        }

        return result;
    }

    private async Task<List<(IProteinPredictionTarget target, string dbPath)>> GetPredictionTargetToExistingDbPathMappingAsync(
        IEnumerable<IProteinPredictionTarget> targetPredictions, IEnumerable<string?> existingDatabaseLocations)
    {
        var resultsLocationMappingToTest = new List<(IProteinPredictionTarget target, string subpath)>();
        var targetToFullResultMapping = new List<(IProteinPredictionTarget target, string dbPath)>();

        foreach (var targetPrediction in targetPredictions)
        {
            resultsLocationMappingToTest.Add((targetPrediction,
                GetMsaResultsSubFolderPathForPrediction(targetPrediction)));
        }

        // each separate database location that has actual entries inside
        foreach (var location in existingDatabaseLocations)
        {
            if (!resultsLocationMappingToTest.Any()) break;

            if (!Directory.Exists(location))
            {
                _logger.LogWarning($"Provided location does not exist: {location}");
                continue;
            }

            List<(IProteinPredictionTarget target, string subpath)> locallyFeasibleMappings = new(resultsLocationMappingToTest);

            // progressively remove larger and larger groups based on test whether its cognate subfolder exists, instead of checking all one by one
            // this helps with nonexisting only
            const int longestAcceptableSublevel = 4;
            const int maxNumberOfPrechecks = 2;
            var numberOfAcceptablyShortLevels = Settings.PersistedA3mDbConfig.FolderOrganizationFragmentLengths
                .Where(x => x <= longestAcceptableSublevel).ToList().Count;
            numberOfAcceptablyShortLevels = Math.Min(numberOfAcceptablyShortLevels, maxNumberOfPrechecks);
            for (int subfolderCount = 1; subfolderCount <= numberOfAcceptablyShortLevels; subfolderCount++)
            {
                var subpaths = locallyFeasibleMappings
                    .Select(x => Helper.GetPathOfFirstNSubpathLevels(x.subpath, subfolderCount)).Distinct().ToList();
                foreach (var subpath in subpaths)
                {
                    var expectedPredictionTargetResultsPath = Path.Join(location, subpath);
                    if (!Directory.Exists(expectedPredictionTargetResultsPath))
                    {
                        locallyFeasibleMappings.RemoveAll(x => x.subpath.StartsWith(subpath));
                    }

                    if (!locallyFeasibleMappings.Any()) break;
                }

                if (!locallyFeasibleMappings.Any()) break;
            }

            if (!locallyFeasibleMappings.Any()) continue;
            
            var parallel = Settings.ComputingConfig.ExistingDatabaseSearchParallelizationFactor;
            var batches = GetBatches(locallyFeasibleMappings, parallel);

            foreach (var batch in batches)
            {
                var taskMapping = new List<(IProteinPredictionTarget target, Task<string?> task)>();
                foreach (var (target, subpath) in batch)
                {
                    var expectedPredictionTargetResultsPath = Path.Join(location, subpath);
                    var task = GetResultFolderWithDesiredResultAsync(target, expectedPredictionTargetResultsPath);
                    taskMapping.Add((target, task));
                }

                try
                {
                    await Task.WhenAll(taskMapping.Select(x => x.task));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Error while checking some of the existing locations.");
                }

                var mapping = taskMapping.Where(x => x.task.IsCompletedSuccessfully && x.task.Result is not null)
                    .Select(x => (target:x.target, fullSubPath:x.task.Result!)).ToList();

                var foundPred = mapping.Select(x => x.target);
                
                targetToFullResultMapping.AddRange(mapping);
                resultsLocationMappingToTest.RemoveAll(x => foundPred.Contains(x.target));
            }
        }

        return targetToFullResultMapping;
    }

    private async Task<(List<IProteinPredictionTarget> existing, List<IProteinPredictionTarget> missing)>
        GetExistingAndMissingPredictionTargetsAsync(List<IProteinPredictionTarget> targetPredictions, IEnumerable<string> existingDatabaseLocations)
    {
        var mapping =
            await GetPredictionTargetToExistingDbPathMappingAsync(targetPredictions, existingDatabaseLocations);

        var targetsWithExistingResults = mapping.Select(x => x.target).ToList();
        var targetsWithMissingResults = targetPredictions.Except(targetsWithExistingResults).ToList();

        return (targetsWithExistingResults, targetsWithMissingResults);
    }

    //TODO: check versions of monos too, not just final a3ms!
    private async Task<(List<Protein> existing, List<Protein> missing)> GetExistingAndMissingProteinsAsync(List<Protein> iproteins, List<string> existingDatabaseLocations)
    {
        var proteinsToCheck = new List<Protein>(iproteins);
        var existing = new List<Protein>();

        foreach (var existingDatabasePath in existingDatabaseLocations)
        {
            if (!proteinsToCheck.Any()) break;

            //TODO: many checks - whether the sequence matches, whether the other stuff apart from qdb exists, ...
            var qdbHeaderDb = Path.Join(existingDatabasePath, Settings.PersistedMonoDbConfig.QdbName) +
                              $"{Mmseqs.Settings.Mmseqs2Internal.DbHeaderSuffix}";

            

            

            var headers = await Mmseqs.GetAllHeadersInSequenceDbHeaderDbAsync(qdbHeaderDb);
            var contained = proteinsToCheck.Where(x => headers.Contains(Helper.GetMd5Hash(x.Sequence))).ToList();
            existing.AddRange(contained);
            proteinsToCheck = proteinsToCheck.Except(existing).ToList();
        }

        var missing = proteinsToCheck.Except(existing).ToList();

        return (existing, missing);
    }

    private string GetMmseqsVersion()
    {
        // shady. TODO: rethink this
#if DEBUGx
        return "fake_version_1.0.0";
#else
        return Mmseqs.GetVersionAsync().GetAwaiter().GetResult();
#endif
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
            var qdbPath = Path.Join(dbLocation, Settings.PersistedMonoDbConfig.QdbName);
            var headersToSearch = uniqueProteins.Select(x => x.Id).ToList();
            resultTasksMapping.Add((dbLocation, Mmseqs.GetHeaderAndIndicesForGivenHeadersInSequenceDbAsync(qdbPath, headersToSearch)));
        }

        var tasks = resultTasksMapping.Select(x => x.resultTask).ToList();
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            var exceptions = tasks.Where(x => x.Exception is not null).ToList();
            if (exceptions.Any())
            {
                _logger.LogWarning($"Reading entries from some locations ({exceptions.Count}/{tasks.Count}) failed unexpectedly.");
            }
        }

        foreach (var (dbPath, resultTask) in resultTasksMapping.Where(x=>x.resultTask.IsCompletedSuccessfully))
        {
            var entriesInDb = resultTask.Result;
            foreach (var (id, indices) in entriesInDb)
            {
                if (!indices.Any()) continue; //this should be impossible though?
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

    private string GetMsaResultsSubFolderPathForPrediction(IProteinPredictionTarget target)
    {
        var predictionHash = Helper.GetAutoHashIdWithoutMultiplicity(target);

        var pathFragments =
            Settings.PersistedA3mDbConfig.FolderOrganizationFragmentLengths.Select(x => predictionHash.Substring(0, x));
        var subPath = Path.Join(pathFragments.ToArray());

        return Path.Join(subPath, predictionHash);
    }

    private List<IProteinPredictionTarget> GetPredictionsThatRequirePairing(List<IProteinPredictionTarget> targets)
    {
        //TODO: add support for explicitly asking for no-pairing (possibly per target?)
        return targets.Where(x => x.IsHeteroComplex).ToList();
    }

    private async Task<List<Protein>> GetProteinTargetsForMonoDbSearchAsync(IEnumerable<string> inputFastaPaths, IEnumerable<string> excludeIds)
    {
        var excludeList = excludeIds.ToList();

        var uniquePreds = (await GetRectifiedTargetPredictionsAsync(inputFastaPaths.ToList())).Distinct();
        var excluded = uniquePreds.Where(x => excludeList.Contains(Helper.GetAutoHashIdWithoutMultiplicity(x))).ToList();
        var proteinsInPreds = uniquePreds.Except(excluded).SelectMany(x => x.UniqueProteins).Distinct().ToList();

        return proteinsInPreds;
    }

    /// <summary>
    /// Will load up targets from the source fasta files, make it so that same prot sequences refer to same Protein instances, keeping duplicates.
    /// Within each target, the ordering will be clearly defined based on constituents, sorted first by sequence length then lexically.
    /// </summary>
    /// <param name="inputPathsList"></param>
    /// <param name="excludedIds"></param>
    /// <returns></returns>
    private async Task<List<IProteinPredictionTarget>> GetRectifiedTargetPredictionsAsync(List<string> inputPathsList)
    {
        var monos = new HashSet<Protein>();

        

        var rectifiedTargets = new List<(string hash, IProteinPredictionTarget target)>();
        var duplicateTargets = new List<IProteinPredictionTarget>();

        IProteinPredictionTargetImporter importer = new HACK_ColabFoldPredictionTargetImporter();

        foreach (var inputFastaPath in inputPathsList)
        {
            await using var stream = File.OpenRead(inputFastaPath);
            var fastaEntries = await FastaHelper.GetFastaEntriesIfValidAsync(stream, SequenceType.Protein, keepCharacters: Settings.ColabfoldComplexFastaMonomerSeparator);
            if (fastaEntries is not null)
            {
                // TODO: this is all dirty and should be refactored at some point. One shouldn't hack up individual elements of Prediction Target like that, that part should be private
                foreach (var fastaEntry in fastaEntries)
                {
                    var prediction =
                        await importer.GetPredictionTargetFromComplexProteinFastaEntry(fastaEntry, Settings.ColabfoldComplexFastaMonomerSeparator);
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
                        var existingPrediction = rectifiedTargets[index].target;
                        duplicateTargets.Add(existingPrediction);
                    }
                    
                    rectifiedTargets.Add((predictionHash, rectifiedPrediction));
                    
                }
            }
        }

        if (duplicateTargets.Any())
        {
            _logger.LogInformation($"Some inputs ({duplicateTargets.Count}) map to identical predictions.");
        }

        var targets = rectifiedTargets.Select(x => x.target).ToList();

        return targets;
    }

    private List<MmseqsPersistedMonoDbEntryFeature> GetRequiredMonoDbFeaturesForPredictions(List<IProteinPredictionTarget> predictionBatch)
    {
        var requiringPairing = GetPredictionsThatRequirePairing(predictionBatch);
        var notRequiringPairing = predictionBatch.Except(requiringPairing).ToList();

        var proteinsRequiringPairing = requiringPairing.SelectMany(x => x.UniqueProteins).Distinct().ToList();
        var proteinsInPairlessTargets = notRequiringPairing.SelectMany(x => x.UniqueProteins).Distinct().ToList();

        var allProteins = proteinsRequiringPairing.Concat(proteinsInPairlessTargets).Distinct().ToList();
        var allFeatures = GetRequiredMonoDbFeaturesForTargets(allProteins);

        var proteinsNotRequiringPairing = proteinsInPairlessTargets.Except(proteinsRequiringPairing).ToList();
        var notNeededFeatures = allFeatures.Where(x => x.SourceType == ColabfoldMsaDataType.Paired
        && proteinsNotRequiringPairing.Contains(x.Mono));

        var finalFeatures = allFeatures.Except(notNeededFeatures).ToList();

        return finalFeatures;
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

    /// <summary>
    /// Result location for a prediction can have multiple results in it for different combos of inputs. This will get all that fit the input target
    /// and the settings of the current program instance
    /// </summary>
    /// <param name="target"></param>
    /// <param name="fullPredictionPathToCheckForResults"></param>
    /// <returns></returns>
    private async IAsyncEnumerable<string> GetResultFoldersWithDesiredResultAsync(Protein target, string persistedMonoLocation)
    {
        var subFolders = await Helper.GetDirectoriesAsync(persistedMonoLocation);
        if (!subFolders.Any()) yield break;

        foreach (var resultFolder in subFolders)
        {
            //if (await Helper.FilesExistParallelAsync())

            var expectedInfoFilePath = Path.Join(resultFolder, Settings.PersistedA3mDbConfig.A3mInfoFilename);
            if (!File.Exists(expectedInfoFilePath)) continue;

            var expectedMsaFilePath = Path.Join(resultFolder, Settings.PersistedA3mDbConfig.ResultA3mFilename);
            if (!File.Exists(expectedMsaFilePath)) continue;

            bool isAcceptable;
            try
            {
                isAcceptable = false; // await CheckIsAcceptablePersistedA3mResultForTargetAsync(target, expectedInfoFilePath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Error while checking {expectedInfoFilePath}, skipping.");
                continue;
            }

            if (isAcceptable) yield return resultFolder;

        }
    }

    /// <summary>
    /// Returns the first found fitting result
    /// </summary>
    /// <param name="target"></param>
    /// <param name="fullPredictionPath"></param>
    /// <returns></returns>
    private async Task<string?> GetResultFolderWithDesiredResultAsync(IProteinPredictionTarget target, string fullPredictionPath)
    {
        if (!Directory.Exists(fullPredictionPath)) return null;

        await foreach (var desiredResultFolder in GetResultFoldersWithDesiredResultAsync(target, fullPredictionPath))
        {
            var firstResult = desiredResultFolder;
            return firstResult;
        }

        return null;

    }

    /// <summary>
    /// Result location for a prediction can have multiple results in it for different combos of inputs. This will get all that fit the input target
    /// and the settings of the current program instance
    /// </summary>
    /// <param name="target"></param>
    /// <param name="fullPredictionPathToCheckForResults"></param>
    /// <returns></returns>
    private async IAsyncEnumerable<string> GetResultFoldersWithDesiredResultAsync(IProteinPredictionTarget target, string fullPredictionPathToCheckForResults)
    {
        var subFolders = await Helper.GetDirectoriesAsync(fullPredictionPathToCheckForResults);
        if (!subFolders.Any()) yield break;

        foreach (var resultFolder in subFolders)
        {
            //if (await Helper.FilesExistParallelAsync())

            var expectedInfoFilePath = Path.Join(resultFolder, Settings.PersistedA3mDbConfig.A3mInfoFilename);
            if (!File.Exists(expectedInfoFilePath)) continue;

            var expectedMsaFilePath = Path.Join(resultFolder, Settings.PersistedA3mDbConfig.ResultA3mFilename);
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
    
    /// <summary>
    /// It finds _one_ result mapping with required features for each feature. There can be any number in the db that are valid, but it will take the first one it runs into,
    /// no particular order enforced. We need _a_ fitting feature, not all of them.
    /// The only issue is the db is corrupt there, repeating the program will keep grabbing the corrupt one. So there's the assumption that the db doesn't have corrupt entries,
    /// which is "ok". Corruption handling can be done elsewhere. User can manually delete the thing based on errors, or ignore the existing db if needed.
    /// </summary>
    /// <param name="persistedMonoDatabasesPaths"></param>
    /// <param name="requiredFeatures"></param>
    /// <param name="existingDbParallelSearchBatchSize"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
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
                    var subFoldersInLocation = (await Helper.GetDirectoriesAsync(location)).Select(Path.GetFileName).ToList();
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

                        // theoretically there be multiple, should never happen but eh. At this point it's a rare enough thing to accept as a weak point
                        var subFolderForTargetDatabase = matchingSubFolders.First()!;
                        var subPath = Path.Join(location, subFolderForTargetDatabase);
                        string requiredDbName;
                        switch (feature.SourceType)
                        {
                            case ColabfoldMsaDataType.Unpaired:
                                requiredDbName = Settings.PersistedMonoDbConfig.MonoA3mDbName;
                                break;
                            case ColabfoldMsaDataType.Paired:
                                requiredDbName = Settings.PersistedMonoDbConfig.ForPairingAlignDbName;
                                break;
                            default:
                                throw new Exception("This should never happen.");
                        }

                        var requiredDbFileName = $"{requiredDbName}{Mmseqs.Settings.Mmseqs2Internal.DbTypeSuffix}";

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

    private List<(int numberOfMonomers, List<IProteinPredictionTarget> predictionTargets)> GroupPredictionsByNumberOfMonomers(List<IProteinPredictionTarget> targets)
    {
        var res = new Dictionary<int, List<IProteinPredictionTarget>>();

        foreach (var predictionTarget in targets)
        {
            var monomerCount = predictionTarget.UniqueProteins.Count;
            if (!res.ContainsKey(monomerCount)) res.Add(monomerCount, new List<IProteinPredictionTarget>());
            res[monomerCount].Add(predictionTarget);
        }

        return res.Select(x=>(x.Key, x.Value)).ToList();
    }
    private bool IsMmseqsHelperDatabaseVersionCompatible(string version)
    {
        //TODO: make this settable by strategy
        if (version == InstanceInfo.HelperDatabaseVersion) return true;

        if (Settings.DatabaseVersionCompatibilityMatrix.ContainsKey(InstanceInfo.HelperDatabaseVersion))
        {
            return Settings.DatabaseVersionCompatibilityMatrix[InstanceInfo.HelperDatabaseVersion].Contains(version);
        }
        return false;
    }

    private async Task<bool> IsValidDbFolder(string path)
    {
        //TODO: instead of true-false, return a judgement enum, so I can report on the reason the folders weren't valid, count, etc

        if (!Directory.Exists(path)) return false;
        
        var files = await Helper.GetFilesAsync(path);
        if (files.Length < Settings.PersistedMonoDbConfig.MinimalNumberOfFilesInResultFolder) return false;
        
        if (!files.Any(x=> Path.GetFileName(x) == Settings.PersistedMonoDbConfig.InfoFilename)) return false;
        
        var infoFilePath = files.Single(x => Path.GetFileName(x) == Settings.PersistedMonoDbConfig.InfoFilename);
        var x = await PersistedMonoDbMetadataInfo.ReadFromFileSystemAsync(infoFilePath);
        if (x is null)
        {
            _logger.LogInformation($"Unable to load the database info for {infoFilePath}, skipping.");
            return false;
        }

        var persistedMonoDbInfo = x!;
        if (!IsMmseqsHelperDatabaseVersionCompatible(persistedMonoDbInfo.MmseqsHelperDatabaseVersion))
        {
            _logger.LogInformation($"Mono db at ({path}) incompatible version ({persistedMonoDbInfo.MmseqsHelperDatabaseVersion}) vs current ({InstanceInfo.HelperDatabaseVersion}).");
            return false;
        }

        // just exploring a bit how structured issue policy implementation might look like... Seems to take a lot of work. And space... 2023-04-17
        if (persistedMonoDbInfo.MmseqsVersion != InstanceInfo.MmseqsVersion)
        {
            var message =
                $"Version of Mmseqs in persisted db ({persistedMonoDbInfo.MmseqsVersion}) and current ({InstanceInfo.MmseqsVersion}) do not match.";
            var policy = Settings.Strategy.MmseqsVersionOfPersistedDatabaseIsLowerThanRunningVersion;
            if (policy.ActionsRequsted.Contains(IssueHandlingAction.Report))
            {
                var logLevel = policy.GetValueOrDefault<LogLevel>("LogLevel", LogLevel.Warning);
                _logger.Log(logLevel, message);
            }

            if (policy.ActionsRequsted.Contains(IssueHandlingAction.SkipCurrentItem))
            {
                _logger.LogInformation("Will not use it.");
                return false;
            }

            if (policy.ActionsRequsted.Contains(IssueHandlingAction.StopProcessing) ||
                policy.ActionsRequsted.Contains(IssueHandlingAction.KillProgram))
            {
                throw new Exception(message);
            }
        }

        //don't think distinct works, it will probably deserialize to separate objects even if the same
        var dbTargetsInResult = persistedMonoDbInfo.DatabaseTargets;

        // if the database contains anything useful, might be fitting
        foreach (var dbTarget in MmseqsSourceDatabaseTargets)
        {
            var matchingDbInPersistedResult = dbTargetsInResult.Where(x =>
            {
                var nameFits = x.Database?.Name?.Equals(dbTarget.Database.Name, StringComparison.OrdinalIgnoreCase) == true;
                var pairingFits = !dbTarget.UseForPaired || dbTarget.UseForPaired && x.UseForPaired;
                var unpairedFits = !dbTarget.UseForUnpaired || dbTarget.UseForUnpaired && x.UseForUnpaired;
                return nameFits && (pairingFits || unpairedFits);
            });

            if (!matchingDbInPersistedResult.Any()) return false;
        }

        // failed to find a reason for it to be invalid
        return true;
    }

    private void LogSomething(string s)
    {
        // var timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
        // Console.WriteLine($"[{timeStamp}]: {s}");
        _logger?.LogInformation(s);
    }

    private async Task<string> PerformPairingForDbTargetAsync(string workingDir, string qdbPath, string pairedAlignDb, MmseqsSourceDatabaseTarget dbTarget)
    {
        //TODO: 2023-04-19 right now it fails if using idx taxonomy - mmseqs still looks for db_taxonomy, instead of db.idx_taxonomy, unlike search, which looks for db.idx_taxonomy
        // I thought to create a soft link to db.idx_taxonomy, but I can't guarantee the process can write to the target location
        // technically I could create a folder with the target db with _all_ the db softlinked, then I could link whatever I want?
        // or, do I patch mmseqs itself?

        var performanceParams = Mmseqs.PerformanceParams(dbTarget.RequestPreloadingToRam);

        var targetDbPathBase = Mmseqs.GetProperDatabaseRootPath(dbTarget.Database.Path);
        var targetDbPathSeq = Mmseqs.GetProperDatabaseSeqPath(dbTarget.Database.Path);
        

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
        await Mmseqs.RunMmseqsAsync(Mmseqs.pairModule, pair1PosParams, $"{performanceParams}");


        //*******************************************align*******************************************************
        var align2ResultDb = Path.Join(localProcessingPath, $"align2");
        var align2PosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            pair1ResultDb,
            align2ResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.alignModule, align2PosParams, $"{Settings.ColabfoldMmseqsParams.Paired.Align2} {performanceParams}");

        //*******************************************pair 1*******************************************************
        var pair2ResultDb = Path.Join(localProcessingPath, $"pair2");
        var pair2PosParams = new List<string>()
        {
            qdbPath,
            targetDbPathBase,
            align2ResultDb,
            pair2ResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.pairModule, pair2PosParams, $"{performanceParams}");

        //*******************************************convert*******************************************************
        var msaConvertResultDb = Path.Join(localProcessingPath, $"pair_a3m");
        var msaConvertPosParams = new List<string>()
        {
            qdbPath,
            targetDbPathSeq,
            pair2ResultDb,
            msaConvertResultDb,
        };
        await Mmseqs.RunMmseqsAsync(Mmseqs.msaConvertModule, msaConvertPosParams, $"{Settings.ColabfoldMmseqsParams.Paired.MsaConvert} {performanceParams}",
            MmseqsAdditionalOptionsForResultsThatGetHandledOutsideOfMmseqs);

        return msaConvertResultDb;
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

