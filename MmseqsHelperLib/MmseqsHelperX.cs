using Microsoft.Extensions.Logging;
using System.Linq;

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
        
        private readonly ILogger _logger;

        public static MmseqsSettings GetDefaultSettings() => new MmseqsSettings();

        public string PerformanceParams => @$"--threads {Settings.ThreadCount} --db-load-mode {(Settings.PreLoadDb ? 2 : 0)}";

        public MmseqsHelper(MmseqsSettings? inputSettings, ILogger logger)
        {
            _logger = logger;
            Settings = inputSettings ?? GetDefaultSettings();
        }

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

        public async Task CreateDbAsync(string queryFastaPath, string outputDbNameBase, CreateDbParameters createDbParameters)
        {
            await CreateDbAsync(new List<string> { queryFastaPath }, outputDbNameBase, createDbParameters);
        }

        public async Task CreateDbAsync(IEnumerable<string> inputPaths, string outputDbNameBase, CreateDbParameters parameters)
        {
            var command = parameters.CommandString;
            var positionalArguments = inputPaths.Append(outputDbNameBase);
            await RunMmseqsAsync(command, positionalArguments, parameters);
        }

        public async Task RunMmseqsAsync(string mmseqsModule, IEnumerable<string> positionalArguments, MmseqsCommandLineParameters nonPositionalParameters)
        {
            var parametersString = String.Join(" ", nonPositionalParameters.GetNonDefault().Select(x => x.GetCommandLineString()));
            await RunMmseqsAsync(mmseqsModule, positionalArguments, parametersString);
        }

        public async Task RunMmseqsAsync(string mmseqsModule, IEnumerable<string> positionalArguments, string nonPositionalParametersString)
        {
            var fullFilePath = Helper.EnsureQuotedIfWhiteSpace(Settings.MmseqsBinaryPath);
            var positionalArgumentsString = String.Join(" ", positionalArguments.Select(Helper.EnsureQuotedIfWhiteSpace));
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


        public async Task<List<(byte[] data, int index)>> ReadEntriesWithIndicesFromDataDbAsync(string dataDbPath, List<int> indices)
        {
            var result = new List<(byte[] data, int index)>();

            var dbIndexFile = $"{dataDbPath}{Settings.Mmseqs2Internal_DbIndexSuffix}";
            var entries = await GetAllIndexFileEntriesInDbAsync(dbIndexFile);
            var orderedEntriesToRead = entries.Where(x => indices.Contains(x.Index)).OrderBy(x => x.StartOffset).ToList();

            var dbDataFile = $"{dataDbPath}{Settings.Mmseqs2Internal_DbDataSuffix}";

            using BinaryReader reader = new BinaryReader(new FileStream(dbDataFile, FileMode.Open));
            foreach (var (index, startOffset, length) in orderedEntriesToRead)
            {
                reader.BaseStream.Position = startOffset;
                var lengthToRead = length - Settings.Mmseqs2Internal_DataEntrySeparator.Length;
                var data = reader.ReadBytes(lengthToRead);
                result.Add((data, index));
            }

            return result;

        }

        public async Task<List<(string header, List<int> indices)>> GetHeaderAndIndicesForGivenHeadersInSequenceDbAsync(string sequenceDbPath, List<string> headersToSearch)
        {
            // will always grab just the first found index when there are duplicates
            var headerFile = $"{sequenceDbPath}{Settings.Mmseqs2Internal_DbHeaderSuffix}";
            var headersInFile = await GetAllHeadersInSequenceDbHeaderDbAsync(headerFile);
            var headerToStartAndLengthMappings = new Dictionary<string, List<MmseqsIndexEntry>>();

            var startOffset = 0;
            foreach (var header in headersInFile)
            {
                var len = header.Length + Settings.Mmseqs2Internal_DataEntrySeparator.Length;
                if (headersToSearch.Contains(header))
                {
                    if (!headerToStartAndLengthMappings.ContainsKey(header))
                    {
                        headerToStartAndLengthMappings.Add(header, new List<MmseqsIndexEntry>());
                    }
                    headerToStartAndLengthMappings[header].Add(new MmseqsIndexEntry(Int32.MinValue, startOffset, len));
                }
                startOffset += len;
            }

            var headerIndexFile = $"{sequenceDbPath}{Settings.Mmseqs2Internal_DbHeaderSuffix}{Settings.Mmseqs2Internal_DbIndexSuffix}";
            var headerIndexFileEntries = await GetAllIndexFileEntriesInDbAsync(headerIndexFile);

            var result = new List<(string header, List<int> indices)>();

            foreach (var (header, matchingEntries) in headerToStartAndLengthMappings)
            {
                if (!matchingEntries.Any()) continue;

                if (matchingEntries.Count > 1)
                {
                    _logger.LogInformation($"Found multiple entries for the same header/id ({header})");
                }
                var fakeIndexEntry = matchingEntries.First();

                var matchedEntryIndices = headerIndexFileEntries.Select((entry, index) => (entry, index))
                    .Where(x=> x.entry.StartOffset == fakeIndexEntry.StartOffset && x.entry.Length == fakeIndexEntry.Length)
                    .Select(x=>x.index).ToList();
                
                if (matchedEntryIndices.Any())
                {
                    result.Add((header, matchedEntryIndices.Select(x=> headerIndexFileEntries[x].Index).ToList()));
                }
            }

            return result;
        }

        private async Task<List<MmseqsIndexEntry>> GetAllIndexFileEntriesInDbAsync(string mmseqsDbIndexFile)
        {
            var allLines = await File.ReadAllLinesAsync(mmseqsDbIndexFile);
            return allLines.Select(x =>
            {
                var entries = x.Split(Settings.Mmseqs2Internal_IndexColumnSeparator);
                return new MmseqsIndexEntry(int.Parse(entries[0]), int.Parse(entries[1]), int.Parse(entries[2]));
            }).ToList();
        }

        public async Task<List<string>> GetIdsFoundInSequenceDbAsync(string dbPath, IEnumerable<string> idsToSearch)
        {
            var headerFile = $"{dbPath}{Settings.Mmseqs2Internal_DbHeaderSuffix}";
            var headersInFile = await GetAllHeadersInSequenceDbHeaderDbAsync(headerFile);
            return idsToSearch.Where(x => headersInFile.Contains(x)).ToList();

        }

        public async Task<List<string>> GetAllHeadersInSequenceDbHeaderDbAsync(string headerDbFile)
        {
            var allText = await File.ReadAllTextAsync(headerDbFile);
            // each line is terminated with newline and ascii null character
            var lines = allText.Split(Settings.Mmseqs2Internal_DataEntrySeparator);
            // last line in collection is empty since even the last line is terminated with newline and null char, remove
            var headers = lines.Take(lines.Length - 1).ToList();
            return headers;
        }

    }
}
