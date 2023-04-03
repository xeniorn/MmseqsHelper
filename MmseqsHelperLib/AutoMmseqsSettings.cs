namespace MmseqsHelperLib;

public class AutoMmseqsSettings
{
    public string PersistedDbFinalA3mExtension { get; init; } = ".a3m";
    public string PersistedDbQdbName { get; init; } = "qdb";
    public string PersistedDbPairModeFirstAlignDbName { get; init; } = "for_pairing_align_mmseqsdb";
    public string PersistedDbMonoModeResultDbName { get; init; } = "final_mono_a3m_mmseqsdb";

    //public string QdbRepositorySubPath { get; init; } = "qdbs";
    //public string PairDbRepositorySubPath { get; init; } = "for_pairing_aligned_dbs";
    //public string MonoDbRepositorySubPath { get; init; } = "mono_dbs";

    public string TempPath { get; set; } = Path.GetTempPath();
    public string Mmseqs2Internal_DbHeaderSuffix { get; init; } = "_h";
    public string Mmseqs2Internal_DbDataSuffix { get; init; } = String.Empty;
    public string Mmseqs2Internal_DbIndexSuffix { get; init; } = ".index";
    public string Mmseqs2Internal_DbLookupSuffix { get; init; }  = ".lookup";
    public string Mmseqs2Internal_DbTypeSuffix { get; init; } = ".dbtype";
    public string Mmseqs2Internal_ExpectedSeqDbSuffix => PreLoadDb ? ".idx" : "_seq";
    public string Mmseqs2Internal_ExpectedAlnDbSuffix => PreLoadDb ? ".idx" : "_aln";
    // manual says only \0 is the separator, but they always use newlines too, and terminal newline does not seem to be a part of the entry... so use it like this for now
    public string Mmseqs2Internal_DataEntrySeparator { get; init; } = "\n\0";
    public string FastaSuffix { get; init; } = ".fasta";
    public Dictionary<string, string> Custom { get; init; } = new();
    public int ThreadCount { get; init; } = 1;
    public bool PreLoadDb { get; init; } = false;
    public int MaxDesiredBatchSize { get; init; } = 1600;
    public string MmseqsBinaryPath { get; set; }
    public int MaxDesiredPredictionTargetBatchSize { get; set; } = 200;
    public string Mmseqs2Internal_IndexColumnSeparator { get; init; } = "\t";
    // (data, index, dbtype) for (mono, pair_align, qdb, qdb_h) = 12, plus qdb.lookup (not needed)
    public int PersistedDbMinimalNumberOfFilesInMonoDbResult { get; init; } = 12;
    public int PairResultDatabaseSubfolderLength { get; init; } = 2;

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