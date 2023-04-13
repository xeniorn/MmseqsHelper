using FastaHelperLib;

namespace MmseqsHelperLib;

internal record MmseqsPersistedMonoDbEntryFeature(Protein Mono, string DatabaseName, ColabfoldMsaDataType SourceType)
{
    public string FeatureSubFolderPath { get; set; } = String.Empty;
    public List<int> Indices { get; set; }
    public string DbPath { get; set; }
}