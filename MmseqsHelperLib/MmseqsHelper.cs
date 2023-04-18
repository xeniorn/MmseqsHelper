using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MmseqsHelperLib
{
    public class MmseqsHelper
    {
        public string searchModule = Constants.ModuleStrings[SupportedMmseqsModule.Search];
        public string moveModule = Constants.ModuleStrings[SupportedMmseqsModule.MoveDatabase];
        public string linkModule = Constants.ModuleStrings[SupportedMmseqsModule.LinkDatabase];
        public string alignModule = Constants.ModuleStrings[SupportedMmseqsModule.Align];
        public string filterModule = Constants.ModuleStrings[SupportedMmseqsModule.FilterResult];
        public string msaConvertModule = Constants.ModuleStrings[SupportedMmseqsModule.ConvertResultToMsa];
        public string expandModule = Constants.ModuleStrings[SupportedMmseqsModule.ExpandAlignment];
        public string mergeModule = Constants.ModuleStrings[SupportedMmseqsModule.MergeDatabases];
        public string pairModule = Constants.ModuleStrings[SupportedMmseqsModule.PairAlign];
        public string versionModule = Constants.ModuleStrings[SupportedMmseqsModule.PrintVersion];

        private readonly ILogger _logger;

        public static MmseqsSettings GetDefaultSettings() => new ();

        public string PerformanceParams(bool isDatabasePreloadedToRam) => 
            @$"--threads {Settings.ThreadsPerProcess} --db-load-mode {(isDatabasePreloadedToRam 
                ? Settings.Mmseqs2Internal.DbLoadModeParameterValueForDatabaseIndexPreloadedToRam
                : Settings.Mmseqs2Internal.DbLoadModeParameterValueForReadDatabaseFromDisk)}";

        public MmseqsHelper(MmseqsSettings? inputSettings, ILogger logger)
        {
            _logger = logger;
            Settings = inputSettings ?? GetDefaultSettings();
            
            if (!Settings.UsePrecalculatedIndex)
            {
                GlobalAdditionalOptions = new NonParametrizedMmseqsOptions();
                GlobalAdditionalOptions.Options.Add(MmseqsNonParametrizedOption.IgnorePrecomputedIndex);
            }
        }

        private NonParametrizedMmseqsOptions? GlobalAdditionalOptions { get; }

        public MmseqsSettings Settings { get; set; }
        

        
        public async Task<(bool success, string resultDbPath)> RunMergeAsync(string qdb, string resultDb, List<string> dbsToMerge, string additionalParameters)
        {
            var mergePositionalParameters = new List<string>()
            {
                qdb,
                resultDb
            };
            mergePositionalParameters.AddRange(dbsToMerge);
            
            try
            {
                await RunMmseqsAsync(mergeModule, mergePositionalParameters, additionalParameters);
            }
            catch (Exception ex)
            {
                return (false, ex.Message);
            }

            return (true, resultDb);

        }
        
        public async Task CreateDbAsync(IEnumerable<string> inputPaths, string outputDbNameBase, CreateDbParameters parameters)
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

        public async Task<string> GetMmseqsResponseAsync(string mmseqsModule, IEnumerable<string> positionalArguments, string nonPositionalParametersString)
        {
            var fullFilePath = Helper.EnsureQuotedIfWhiteSpace(Settings.MmseqsBinaryPath);
            var positionalArgumentsString = String.Join(" ", positionalArguments.Select(Helper.EnsureQuotedIfWhiteSpace));
            var processArgumentsString = $"{mmseqsModule} {positionalArgumentsString} {nonPositionalParametersString}";

            LogSomething($"{fullFilePath} {processArgumentsString}");

            using var outStream = new MemoryStream();
            using var errStream = new MemoryStream();

            var (exitCode, output)  = await Helper.RunProcessAndGetStdoutAsync(fullFilePath, processArgumentsString);

            const int successExit = 0;
            if (exitCode != successExit)
                throw new Exception(
                    $"Return: {exitCode}. Failed to run mmseqs {fullFilePath}; {processArgumentsString}.");

            return output;
        }


        public async Task RunMmseqsAsync(string mmseqsModule, IEnumerable<string> positionalArguments, string nonPositionalParametersString, NonParametrizedMmseqsOptions? additionalOptions = null)
        {
            var fullFilePath = Helper.EnsureQuotedIfWhiteSpace(Settings.MmseqsBinaryPath);
            var positionalArgumentsString = String.Join(" ", positionalArguments.Select(Helper.EnsureQuotedIfWhiteSpace));
            var processArgumentsString = $"{mmseqsModule} {positionalArgumentsString} {nonPositionalParametersString}";

            Dictionary<string, string>? envVarsToSet = null;

            if (GlobalAdditionalOptions is not null || additionalOptions is not null)
            {
                var options = NonParametrizedMmseqsOptions.Combine(new[] { GlobalAdditionalOptions, additionalOptions })!;
                if (options.Options.Any())
                {
                    envVarsToSet = new Dictionary<string, string>();
                    if (options.Options.Contains(MmseqsNonParametrizedOption.ForceMergingOfOutputDatabases))
                    {
                        envVarsToSet.Add(Settings.Mmseqs2Internal.EnvVar_ForceDatabaseMerging_Name, Settings.Mmseqs2Internal.EnvVar_ForceDatabaseMerging_EnablingValue);
                    };
                    if (options.Options.Contains(MmseqsNonParametrizedOption.IgnorePrecomputedIndex))
                    {
                        envVarsToSet.Add(Settings.Mmseqs2Internal.EnvVar_IgnorePrecomputedIndex_Name, Settings.Mmseqs2Internal.EnvVar_IgnorePrecomputedIndex_EnablingValue);
                    };
                }
            }

            LogSomething($"{fullFilePath} {processArgumentsString}");
#if DEBUGx 
            return;
#endif

            var exitCode = await Helper.RunProcessAsync(fullFilePath, processArgumentsString, envVarsToSet);

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

            await Helper.CreateDirectoryAsync(destinationFolder);

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

        //public async Task CopyDatabaseAsync(string sourceDbNameBase, string targetDbNameBase, string sourceFolder, string destinationFolder)
        //{
        //    if (!IsValidDbName(sourceDbNameBase) || !IsValidDbName(targetDbNameBase))
        //    {
        //        throw new ArgumentException("Invalid Db name given");
        //    }

        //    if (sourceFolder.Any(x => Path.GetInvalidPathChars().Contains(x)) || destinationFolder.Any(x => Path.GetInvalidPathChars().Contains(x)))
        //    {
        //        throw new ArgumentException("Invalid folder given");
        //    }

        //    if (!Directory.Exists(sourceFolder))
        //    {
        //        throw new ArgumentException("Source folder does not exist");
        //    }

        //    await Helper.CreateDirectoryAsync(destinationFolder);

        //    var possibleExtensions = Settings.PossibleDbExtensions();
        //    var tasks = new List<Task>();

        //    foreach (var extension in possibleExtensions.Values)
        //    {
        //        var sourceFilename = $"{sourceDbNameBase}{extension}";
        //        var targetFilename = $"{targetDbNameBase}{extension}";
        //        tasks.Add(Helper.CopyFileIfExistsAsync(Path.Join(sourceFolder, sourceFilename), Path.Join(destinationFolder, targetFilename)));
        //    }

        //    await Task.WhenAll(tasks);

        //}

        //private bool IsValidDbName(string sourceDbNameBase)
        //{
        //    return !sourceDbNameBase.Any(x => Path.GetInvalidFileNameChars().Contains(x) || char.IsWhiteSpace(x));
        //}

        public async Task CopyDatabaseAsync(string sourceDbPathWithDatabaseRootName, string targetDbPathWithDatabaseRootName)
        {
            var sourceDir = Path.GetDirectoryName(sourceDbPathWithDatabaseRootName) ?? String.Empty;
            var sourceDb = Path.GetFileName(sourceDbPathWithDatabaseRootName);

            var targetDir = Path.GetDirectoryName(targetDbPathWithDatabaseRootName) ?? String.Empty;
            var targetDb = Path.GetFileName(targetDbPathWithDatabaseRootName);

            await CopyDatabaseAsync(sourceDb, targetDb, sourceDir, targetDir);
        }


        private bool IsValidDbName(string sourceDbNameBase)
        {
            return !sourceDbNameBase.Any(x => Path.GetInvalidFileNameChars().Contains(x) || char.IsWhiteSpace(x));
        }


        public async Task<(bool success, string linkedDbPath)> RunLinkDbAsync(string sourceDb, string targetDb, string additionalParameters = "")
        {
            var linkPosParams = new List<string>() { sourceDb, targetDb };

            try
            {
                await RunMmseqsAsync(linkModule, linkPosParams, additionalParameters);
            }
            catch (Exception ex)
            {
                return (false, ex.Message);
            }

            return (true, targetDb);

        }


        /// <summary>
        /// This is agnostic to data type, it will read the data as-is. The only thing stripped is the data terminator, that's constant to all databases
        /// </summary>
        /// <param name="dataDbPath"></param>
        /// <param name="indices"></param>
        /// <returns></returns>
        public async Task<List<(byte[] data, int index)>> ReadEntriesWithIndicesFromDataDbAsync(string dataDbPath, List<int> indices)
        {
            var result = new List<(byte[] data, int index)>();

            var dbIndexFile = $"{dataDbPath}{Settings.Mmseqs2Internal.DbIndexSuffix}";
            var entries = await GetAllIndexFileEntriesInDbAsync(dbIndexFile);
            
            var emptyEntryLength = Settings.Mmseqs2Internal.DataEntryTerminator.Length;

            List<MmseqsIndexEntry> orderedEntriesToRead;

            // need not to skip empty stuff because an empty entry also has a meaning ("no results") - different from absence of entry!
            const bool defaultSkipEmpty = false;

            if (defaultSkipEmpty)
            {
                orderedEntriesToRead = entries
                    .Where(x =>
                        indices.Contains(x.Index)
                        && x.Length > emptyEntryLength)
                    .OrderBy(x => x.StartOffset).ToList();
            }
            else
            {
                orderedEntriesToRead = entries
                    .Where(x =>
                        indices.Contains(x.Index))
                    .OrderBy(x => x.StartOffset).ToList();
            }
            var dbDataFile = $"{dataDbPath}{Settings.Mmseqs2Internal.DbDataSuffix}";

            using BinaryReader reader = new BinaryReader(new FileStream(dbDataFile, FileMode.Open));
            foreach (var (index, startOffset, length) in orderedEntriesToRead)
            {
                if (length <= emptyEntryLength)
                {
                    result.Add((Array.Empty<byte>(), index));
                    continue;
                }
                
                reader.BaseStream.Position = startOffset;
                var lengthToRead = length - Settings.Mmseqs2Internal.DataEntryTerminator.Length;
                var data = reader.ReadBytes(lengthToRead);
                result.Add((data, index));
            }

            return result;

        }

        // allows duplicate inputs, but will give non-duplicated output
        public async Task<List<(string header, List<int> indices)>> 
            GetHeaderAndIndicesForGivenHeadersInSequenceDbAsync(string sequenceDbPath, List<string> headersToSearch)
        {
            // will always grab just the first found index when there are duplicates
            var headerFile = $"{sequenceDbPath}{Settings.Mmseqs2Internal.DbHeaderSuffix}";
            var headersInFile = await GetAllHeadersInSequenceDbHeaderDbAsync(headerFile);
            var headerToFakeIndexEntryMapping = new Dictionary<string, List<MmseqsIndexEntry>>();

            var uniqueHeaders = headersToSearch.Distinct().ToList();

            long startOffset = 0;
            foreach (var header in headersInFile)
            {
                var expectedEntryLength = header.Length 
                                  + Settings.Mmseqs2Internal.HeaderEntryHardcodedSuffix.Length 
                                  + Settings.Mmseqs2Internal.DataEntryTerminator.Length;
                if (uniqueHeaders.Contains(header))
                {
                    if (!headerToFakeIndexEntryMapping.ContainsKey(header))
                    {
                        headerToFakeIndexEntryMapping.Add(header, new List<MmseqsIndexEntry>());
                    }
                    //TODO: ehhhh... that's too hacky for my taste but ok. The hackiness is contained within this method, fake value replaced below
                    const int fakeIndexThatMustDefinitelyBeReplacedLater = Int32.MaxValue;
                    headerToFakeIndexEntryMapping[header].Add(new MmseqsIndexEntry(fakeIndexThatMustDefinitelyBeReplacedLater, startOffset, expectedEntryLength));
                }
                startOffset += expectedEntryLength;
            }

            var headerIndexFile = $"{sequenceDbPath}{Settings.Mmseqs2Internal.DbHeaderSuffix}{Settings.Mmseqs2Internal.DbIndexSuffix}";
            var headerIndexEntries = await GetAllIndexFileEntriesInDbAsync(headerIndexFile);

            var result = new List<(string header, List<int> indices)>();

            foreach (var (header, fakeExpectedEntries) in headerToFakeIndexEntryMapping)
            {
                if (!fakeExpectedEntries.Any()) continue;

                if (fakeExpectedEntries.Count > 1)
                {
                    _logger.LogInformation($"Found multiple entries for the same header/id ({header})");
                }
                var fakeIndexEntry = fakeExpectedEntries.First();

                var matchedIndexEntries = headerIndexEntries.Where(x=>
                    x.StartOffset == fakeIndexEntry.StartOffset
                    && x.Length == fakeIndexEntry.Length).ToList();

                if (matchedIndexEntries.Any())
                {
                    var indexList = matchedIndexEntries.Select(x => x.Index).ToList();
                    result.Add((header, indexList));
                }
            }

            return result;
        }

        private async Task<List<MmseqsIndexEntry>> GetAllIndexFileEntriesInDbAsync(string mmseqsDbIndexFile)
        {
            var allLines = await File.ReadAllLinesAsync(mmseqsDbIndexFile);
            return allLines.Select(x =>
            {
                var entries = Helper.RemoveSuffix(x, Settings.Mmseqs2Internal.IndexEntryTerminator)
                    .Split(Settings.Mmseqs2Internal.IndexIntraEntryColumnSeparator);
                return new MmseqsIndexEntry(int.Parse(entries[0]), long.Parse(entries[1]), int.Parse(entries[2]));
            }).ToList();
        }

        public async Task<List<string>> GetIdsFoundInSequenceDbAsync(string dbPath, IEnumerable<string> idsToSearch)
        {
            var headerFile = $"{dbPath}{Settings.Mmseqs2Internal.DbHeaderSuffix}";
            var headersInFile = await GetAllHeadersInSequenceDbHeaderDbAsync(headerFile);
            return idsToSearch.Where(x => headersInFile.Contains(x)).ToList();

        }

        // TODO: rewrite this using byte arrays or similar
        public async Task<List<string>> GetAllHeadersInSequenceDbHeaderDbAsync(string headerDbFile)
        {
            var allText = await File.ReadAllTextAsync(headerDbFile);
            
            // each entry is terminated by ascii null character for all mmseqs dbs
            var lines = allText.Split(Settings.Mmseqs2Internal.DataEntryTerminator);

            // in header db, headers are by convention written out with a terminating newline
            var headers = lines.Select(x => 
                    Helper.RemoveSuffix(x, Settings.Mmseqs2Internal.HeaderEntryHardcodedSuffix))
                .ToList();

            return headers;
        }

        public async Task<string> GetVersionAsync()
        {
            var response = await GetMmseqsResponseAsync(versionModule, new List<string>(), string.Empty);
            return response.Trim();
        }

        public static NonParametrizedMmseqsOptions GetOptionsThatEnsureMergedResults()
        {
            return new NonParametrizedMmseqsOptions()
            {
                Options = new HashSet<MmseqsNonParametrizedOption>()
                {
                    MmseqsNonParametrizedOption.ForceMergingOfOutputDatabases
                }
            };
        }

        public string GetProperDatabaseRootPath(string naiveDbPath)
        {
            if (Settings.UsePrecalculatedIndex)
            {
                return $"{naiveDbPath}{Settings.Mmseqs2Internal.PrecalculatedIndexSuffix}";
            }
            else
            {
                return naiveDbPath;
            }
        }

        public string GetProperDatabaseSeqPath(string naiveDbPath)
        {
            if (Settings.UsePrecalculatedIndex)
            {
                return $"{naiveDbPath}{Settings.Mmseqs2Internal.PrecalculatedIndexSuffix}";
            }
            else
            {
                return $"{naiveDbPath}{Settings.Mmseqs2Internal.SourceDatabaseSequenceSuffix}"; ;
            }
        }

        public string GetProperDatabaseAlignPath(string naiveDbPath)
        {
            if (Settings.UsePrecalculatedIndex)
            {
                return $"{naiveDbPath}{Settings.Mmseqs2Internal.PrecalculatedIndexSuffix}";
            }
            else
            {
                return $"{naiveDbPath}{Settings.Mmseqs2Internal.SourceDatabaseAlignmentSuffix}"; ;
            }
        }


    }

    public enum MmseqsNonParametrizedOption
    {
        ForceMergingOfOutputDatabases,
        IgnorePrecomputedIndex
    }

    public class NonParametrizedMmseqsOptions
    {
        public HashSet<MmseqsNonParametrizedOption> Options { get; set; } = new();

        public static NonParametrizedMmseqsOptions? Combine(IEnumerable<NonParametrizedMmseqsOptions?> sourceOptions)
        {
            NonParametrizedMmseqsOptions? res = null;
            
            foreach (var options in sourceOptions)
            {
                if (options is null) continue;
                if (res is null) res = new NonParametrizedMmseqsOptions();

                foreach (var option in options.Options)
                {
                    res.Options.Add(option);
                }
            }

            return res;
        }
    }

}
