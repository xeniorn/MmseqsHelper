using FastaHelperLib;

namespace MmseqsHelperLib;

public class HACK_ColabFoldPredictionTargetImporter : IProteinPredictionTargetImporter
{
    public const string defaultComplexSplitter = ":";

    public async Task<IProteinPredictionTarget> GetPredictionTargetFromComplexProteinFastaEntry(FastaEntry fastaEntry, string complexSplitter = defaultComplexSplitter)
    {
        //TODO: the rectified IDs can become identical even when the source wasn't identical. Possibly should try

        var subsequences = fastaEntry.Sequence.Split(complexSplitter).ToList();

        var uniqueSeq = subsequences.Distinct().ToList();
        var multiplicities = uniqueSeq.Select(refSeq => subsequences.Count(seq => Equals(seq, refSeq))).ToList();

        var id = fastaEntry.HeaderWithoutSymbol;

        var IProteinPredictionTarget = new ColabfoldPredictionTarget(multiplicities: multiplicities,
            uniqueProteins: uniqueSeq.Select(x => new Protein() { Sequence = x }).ToList(), userProvidedId: id);
        return IProteinPredictionTarget;
    }
}