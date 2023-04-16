namespace MmseqsHelperLib;

public class MmseqsSettings
{
    public MmseqsSettings()
    {
        Mmseqs2Internal = new MmseqsInternalConfigurationSettings();
        //Mmseqs2Internal.ToJson();
    }
    
    public bool PreLoadDb { get; set; } = false;
    public bool UsePrecalculatedIndex { get; set; } = true;

    public MmseqsInternalConfigurationSettings Mmseqs2Internal { get; set; }

    public string ExpectedSeqDbSuffix => UsePrecalculatedIndex ? Mmseqs2Internal.PrecalculatedIndexSuffix : Mmseqs2Internal.SourceDatabaseSequenceSuffix;
    public string ExpectedAlnDbSuffix => UsePrecalculatedIndex ? Mmseqs2Internal.PrecalculatedIndexSuffix : Mmseqs2Internal.SourceDatabaseAlignmentSuffix;
    
    public string TempPath { get; set; } = Path.GetTempPath();

    public string MmseqsBinaryPath { get; set; }

    public int ThreadsPerProcess { get; set; } = 1;
    

    public Dictionary<string, string> PossibleDbExtensions()
    {
        return new Dictionary<string, string>()
        {
            { "data", $"{Mmseqs2Internal.DbDataSuffix}" },
            { "data.index", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbIndexSuffix}" },
            { "data.dbtype", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbTypeSuffix}" },
            { "data_header", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbHeaderSuffix}" },
            { "data_header.index", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbHeaderSuffix}{Mmseqs2Internal.DbIndexSuffix}" },
            { "data_header.dbtype", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbHeaderSuffix}{Mmseqs2Internal.DbTypeSuffix}" },
            { "data.lookup", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbLookupSuffix}" }
        };
    }

}