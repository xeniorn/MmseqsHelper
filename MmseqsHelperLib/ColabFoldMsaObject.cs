using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AlphafoldPredictionLib;
using FastaHelperLib;

namespace MmseqsHelperLib;

public class ColabFoldMsaObject
{
    public ColabFoldMsaObject(List<AnnotatedMsaData> dataEntries, PredictionTarget predictionTarget)
    {
        DataEntries = dataEntries;
        PredictionTarget = predictionTarget;
    }

    public List<AnnotatedMsaData> DataEntries { get; set; } = new ();
    public PredictionTarget PredictionTarget { get; }
    private ColabfoldMsaMetadataInfo Metadata { get; set; }
    
    public async Task WriteToFileSystemAsync(AutoColabfoldMmseqsSettings settings, string targetFolder)
    {
        Metadata = new ColabfoldMsaMetadataInfo(predictionTarget: PredictionTarget, createTime: DateTime.Now,
            mmseqsHelperDatabaseVersion: settings.ColabfoldMmseqsHelperDatabaseVersion,
            mmseqsVersion: settings.MmseqsVersion);

        var fullMsaPath = Path.Join(targetFolder, settings.PersistedDbFinalA3mName);
        var fullInfoPath = Path.Join(targetFolder, settings.PersistedDbFinalA3mInfoName);
        var writeTasks = new List<Task>()
        {
            File.WriteAllBytesAsync(fullMsaPath, GetBytes()),
            Metadata.WriteToFileSystemAsync(fullInfoPath)
        };
        await Task.WhenAll(writeTasks);
    }

    private byte[] GetBytes()
    {
        var text = GetString();
        var bytes = Encoding.ASCII.GetBytes(text);
        return bytes;
    }
    private IEnumerable<string> GetPairedLines(Dictionary<Protein, byte[]> pairedData)
    {
        if (!pairedData.Any()) yield break;

        var proteinToPairedLineList = new List<(Protein protein, string[] a3mLines)>();
        foreach (var (protein, bytes) in pairedData)
        {
            var lines = Encoding.ASCII.GetString(bytes).Split("\n");
            proteinToPairedLineList.Add((protein, lines));
        }

        var lineCount = proteinToPairedLineList.First().a3mLines.Length;
        if (proteinToPairedLineList.Any(x => x.a3mLines.Length != lineCount))
        {
            throw new ArgumentException("Paired reads have unequal number of lines, this should not happen.");
        }
        // each entry has two lines; header and data
        var entryCount = lineCount / 2;

        for (var entry = 0; entry < entryCount; entry++)
        {
            const char FastaHeaderSymbol = '>';

            var headerIndex = 2 * entry;
            var dataIndex = headerIndex + 1;

            var headerLine = FastaHeaderSymbol + string.Join("\t", proteinToPairedLineList.Select(x => x.a3mLines[headerIndex].Trim().TrimStart(FastaHeaderSymbol)));
            var dataLine = string.Join("", proteinToPairedLineList.Select(x => x.a3mLines[dataIndex]));

            yield return headerLine;
            yield return dataLine;
        }
    }

    private string GetString()
    {
        var lines = new List<string>();
        var commentLine = $"#{String.Join(",", PredictionTarget.UniqueProteins.Select(x => x.Sequence.Length))}\t{String.Join(",", PredictionTarget.Multiplicities)}";
        lines.Add(commentLine);

        foreach (var entry in DataEntries.Where(x=>x.DataType == ColabfoldMsaDataType.Paired))
        {
            var nextAvailableLine = lines.Count();
            var pairLines = GetPairedLines(entry.DataDict).ToList();
            
            var startLine = nextAvailableLine;
            var endLine = nextAvailableLine + pairLines.Count - 1;
            var isEmpty = !pairLines.Any();
            Metadata.MsaOriginDefinitions.Add(new MsaOriginDefinition(startLine, endLine, entry.SourceDatabaseTarget, 
                true, isEmpty));
            lines.AddRange(pairLines);
        }

        foreach (var entry in DataEntries.Where(x => x.DataType == ColabfoldMsaDataType.Unpaired))
        {
            var nextAvailableLine = lines.Count();
            var unpairedLines = GetUnpairedLines(entry.DataDict).ToList();

            var startLine = nextAvailableLine;
            var endLine = nextAvailableLine + unpairedLines.Count - 1;
            var isEmpty = !unpairedLines.Any();
            Metadata.MsaOriginDefinitions.Add(new MsaOriginDefinition(startLine, endLine, entry.SourceDatabaseTarget,
                false, isEmpty));
            lines.AddRange(unpairedLines);
        }
        
        // don't forget the terminal newline
        return $"{string.Join("\n", lines)}\n";

    }

    private IEnumerable<string> GetUnpairedLines(Dictionary<Protein, byte[]> unpairedData)
    {
        foreach (var predictionTargetUniqueProtein in PredictionTarget.UniqueProteins)
        {
            const char fastaGapSymbol = '-';

            var unpairedLineList = Encoding.ASCII.GetString(unpairedData[predictionTargetUniqueProtein]).Split("\n");

            var index = PredictionTarget.UniqueProteins.IndexOf(predictionTargetUniqueProtein);
            var lengthProteinsBefore = PredictionTarget.UniqueProteins.Where((_, i) => i < index).Select(x => x.Sequence.Length).Sum();
            var lengthProteinsAfter = PredictionTarget.UniqueProteins.Where((_, i) => i > index).Select(x => x.Sequence.Length).Sum();

            var dataPreAppendGap = new string(fastaGapSymbol, lengthProteinsBefore);
            var dataPostAppendGap = new string(fastaGapSymbol, lengthProteinsAfter);

            var lineCount = unpairedLineList.Length;
            var entryCount = lineCount / 2;

            for (var entry = 0; entry < entryCount; entry++)
            {
                var headerIndex = 2 * entry;
                var dataIndex = headerIndex + 1;

                var headerLine = unpairedLineList[headerIndex].Trim();
                var dataLine = string.Join("", dataPreAppendGap, unpairedLineList[dataIndex], dataPostAppendGap);

                yield return headerLine;
                yield return dataLine;
            }
        }
    }
}