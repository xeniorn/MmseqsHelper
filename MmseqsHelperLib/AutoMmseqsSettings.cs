namespace MmseqsHelperLib;

public class AutoMmseqsSettings
{
    public string QdbName { get; init; } = "qdb";
    public string PairModeFirstAlignDbName { get; init; } = "for_pairing_align_mmseqsdb";
    public string MonoModeResultDbName { get; init; } = "final_mono_a3m_mmseqsdb";

    public string QdbRepositorySubPath { get; init; } = "qdbs";
    public string PairDbRepositorySubPath { get; init; } = "for_pairing_aligned_dbs";
    public string MonoDbRepositorySubPath { get; init; } = "mono_dbs";

    public string TempPath { get; set; } = Path.GetTempPath();
    public string Mmseqs2Internal_DbHeaderSuffix { get; init; } = "_h";
    public string Mmseqs2Internal_DbDataSuffix { get; init; } = String.Empty;
    public string Mmseqs2Internal_DbIndexSuffix { get; init; } = ".index";
    public string Mmseqs2Internal_DbLookupSuffix { get; init; }  = ".lookup";
    public string Mmseqs2Internal_DbTypeSuffix { get; init; } = ".dbtype";
    public string Mmseqs2Internal_ExpectedSeqDbSuffix => PreLoadDb ? ".idx" : "_seq";
    public string Mmseqs2Internal_ExpectedAlnDbSuffix => PreLoadDb ? ".idx" : "_aln";
    public string FastaSuffix { get; init; } = ".fasta";
    public Dictionary<string, string> Custom { get; init; } = new();
    public int ThreadCount { get; init; } = 1;
    public bool PreLoadDb { get; init; } = false;
    public int MaxDesiredBatchSize { get; init; } = 1600;
    public string MmseqsBinaryPath { get; set; }

    public Dictionary<string,string> PossibleDbExtensions()
    {
        return new Dictionary<string, string>()
        {
            { "data", $"{Mmseqs2Internal_DbDataSuffix}" },
            { "data.index", $"{Mmseqs2Internal_DbDataSuffix}{Mmseqs2Internal_DbIndexSuffix}" },
            { "data.dbtype", $"{Mmseqs2Internal_DbDataSuffix}{Mmseqs2Internal_DbTypeSuffix}" },
            { "data_header", $"{Mmseqs2Internal_DbDataSuffix}{Mmseqs2Internal_DbHeaderSuffix}" },
            { "data_header.index", $"{Mmseqs2Internal_DbDataSuffix}{Mmseqs2Internal_DbHeaderSuffix}{Mmseqs2Internal_DbIndexSuffix}" },
            { "data_header.dbtype", $"{Mmseqs2Internal_DbDataSuffix}{Mmseqs2Internal_DbHeaderSuffix}{Mmseqs2Internal_DbTypeSuffix}" },
            { "data.lookup", $"{Mmseqs2Internal_DbDataSuffix}{Mmseqs2Internal_DbLookupSuffix}" }
        };
    }

}