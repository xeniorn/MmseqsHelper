namespace MmseqsHelperLib;

public class MmseqsSettings
{
    public bool PreLoadDb { get; init; } = false;

    public string Mmseqs2Internal_DbHeaderSuffix { get; init; } = @"_h";
    public string Mmseqs2Internal_DbDataSuffix { get; init; } = String.Empty;
    public string Mmseqs2Internal_DbIndexSuffix { get; init; } = @".index";
    public string Mmseqs2Internal_DbLookupSuffix { get; init; } = @".lookup";
    public string Mmseqs2Internal_DbTypeSuffix { get; init; } = @".dbtype";
    public string Mmseqs2Internal_ExpectedSeqDbSuffix => PreLoadDb ? ".idx" : "_seq";
    public string Mmseqs2Internal_ExpectedAlnDbSuffix => PreLoadDb ? ".idx" : "_aln";

    // manual says only \0 is the separator, but they always use newlines too, and terminal newline does not seem to be a part of the entry... so use it like this for now
    // it all really breaks apart when this stuff isn't exact. All entries must be \n terminated for realz.
    public string Mmseqs2Internal_DataEntrySeparator { get; init; } = "\n\0";
    public string Mmseqs2Internal_IndexColumnSeparator { get; init; } = "\t";
    public string Mmseqs2Internal_LookupColumnSeparator { get; init; } = "\t";
    public string Mmseqs2Internal_IndexEntryTerminator { get; init; } = "\n";
    public string Mmseqs2Internal_LookupEntryTerminator { get; init; } = "\n";
    
    public string TempPath { get; set; } = Path.GetTempPath();

    public string MmseqsBinaryPath { get; set; }

    public int ThreadCount { get; init; } = 1;
    

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