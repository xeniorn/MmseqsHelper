using System.Text;
using AlphafoldPredictionLib;
using FastaHelperLib;

namespace MmseqsHelperLib;

public class ColabFoldMsaObject
{
    public ColabFoldMsaObject(Dictionary<Protein, byte[]> pairedData, Dictionary<Protein, byte[]> unpairedData, PredictionTarget predictionTarget)
    {
        PairedData = pairedData;
        UnpairedData = unpairedData;
        PredictionTarget = predictionTarget;

        var individualSequenceHashes = PredictionTarget.UniqueProteins.Select(x => Helper.GetMd5Hash(x.Sequence));
        var sequenceBasedHashBasis = string.Join(":", individualSequenceHashes);
        HashId = Helper.GetMd5Hash(sequenceBasedHashBasis);

    }

    public string HashId { get; }

    public byte[] GetBytes()
    {
        var text = GetString();
        var bytes = Encoding.ASCII.GetBytes(text);
        return bytes;
    }

    private string GetString()
    {
        var lines = new List<string>();
            
        var commentLine = $"#{String.Join(",", PredictionTarget.UniqueProteins.Select(x => x.Sequence.Length))}\t{String.Join(",", PredictionTarget.Multiplicities)}";
        lines.Add(commentLine);
        lines.AddRange(GetPairLines());
        foreach (var predictionTargetUniqueProtein in PredictionTarget.UniqueProteins)
        {
            lines.AddRange(GetUnpairedLines(predictionTargetUniqueProtein));
        }

        return string.Join("\n", lines);

    }

    private IEnumerable<string> GetUnpairedLines(Protein predictionTargetUniqueProtein)
    {
        const char fastaGapSymbol = '-';

        var unpairedLineList = Encoding.ASCII.GetString(UnpairedData[predictionTargetUniqueProtein]).Split("\n");
            
        var lineParts = new List<string>();

        var index = PredictionTarget.UniqueProteins.IndexOf(predictionTargetUniqueProtein);
        var lengthProteinsBefore = PredictionTarget.UniqueProteins.TakeWhile((x,i)=>i<index).Select(x=>x.Sequence.Length).Sum();
        var lengthProteinsAfter = PredictionTarget.UniqueProteins.TakeWhile((x, i) => i > index).Select(x => x.Sequence.Length).Sum();

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

    private IEnumerable<string> GetPairLines()
    {
        var proteinToPairedLineList = new List<(Protein protein, string[] a3mLines)>();
        foreach (var (protein, bytes) in PairedData)
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

    private Dictionary<Protein, byte[]> PairedData { get; }
    private Dictionary<Protein, byte[]> UnpairedData { get;  }

    public PredictionTarget PredictionTarget { get; }
}