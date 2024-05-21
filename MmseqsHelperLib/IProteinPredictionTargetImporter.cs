//#define DIRECTIVE_SYMBOL_ALLOW_INTRA_CHAIN_SKIP
//#define 

using FastaHelperLib;

namespace MmseqsHelperLib;

public interface IProteinPredictionTargetImporter
{
    public Task<IProteinPredictionTarget> GetPredictionTargetFromComplexProteinFastaEntry(FastaEntry fastaEntry, string complexSplitter);
}