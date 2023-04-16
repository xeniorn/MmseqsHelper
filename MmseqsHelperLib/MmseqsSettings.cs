namespace MmseqsHelperLib;

public class MmseqsSettings
{
    
    public bool PreLoadDb { get; set; } = false;

    public string Mmseqs2Internal_DbHeaderSuffix { get; init; } = @"_h";
    public string Mmseqs2Internal_DbDataSuffix { get; init; } = String.Empty;
    public string Mmseqs2Internal_DbIndexSuffix { get; init; } = @".index";
    public string Mmseqs2Internal_DbLookupSuffix { get; init; } = @".lookup";
    public string Mmseqs2Internal_DbTypeSuffix { get; init; } = @".dbtype";
    public string Mmseqs2Internal_ExpectedSeqDbSuffix => PreLoadDb ? ".idx" : "_seq";
    public string Mmseqs2Internal_ExpectedAlnDbSuffix => PreLoadDb ? ".idx" : "_aln";

    // manual says only \0 is the separator and zero-length entries indeed don't have a newline
    public string Mmseqs2Internal_DataEntryTerminator { get; init; } = "\0";
    // newlines separate each index entry
    public string Mmseqs2Internal_IndexEntryTerminator { get; init; } = "\n";
    // newlines separate each lookup entry
    public string Mmseqs2Internal_LookupEntryTerminator { get; init; } = "\n";
    // headers and sequences have hardcoded newlines as a suffix
    public string Mmseqs2Internal_HeaderEntryHardcodedSuffix { get; init; } = "\n";
    // headers and sequences have hardcoded newlines as a suffix
    public string Mmseqs2Internal_SequenceEntryHardcodedSuffix { get; init; } = "\n";

    // within each entry, tabs are used, e.g.
    // 0    0   15
    // 1    15  15
    public string Mmseqs2Internal_IndexIntraEntryColumnSeparator { get; init; } = "\t";

    // within each entry, tabs are used, e.g.
    // 0    headerX    0
    // 1    headerX    0
    public string Mmseqs2Internal_LookupIntraEntryColumnSeparator { get; init; } = "\t";

    public string TempPath { get; set; } = Path.GetTempPath();

    public string MmseqsBinaryPath { get; set; }

    public int ThreadCount { get; set; } = 1;
    

    public Dictionary<string, string> PossibleDbExtensions()
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