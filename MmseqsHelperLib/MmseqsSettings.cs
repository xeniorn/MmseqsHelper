namespace MmseqsHelperLib;

public class MmseqsSettings
{
    public MmseqsSettings()
    {
        Mmseqs2Internal = new MmseqsInternalConfigurationSettings();
        //Mmseqs2Internal.ToJson();
    }
    
    public bool UsePrecalculatedIndex { get; set; } = true;

    public MmseqsInternalConfigurationSettings Mmseqs2Internal { get; set; }

    public string ExpectedSeqDbSuffix => UsePrecalculatedIndex ? Mmseqs2Internal.PrecalculatedIndexSuffix : Mmseqs2Internal.SourceDatabaseSequenceSuffix;
    public string ExpectedAlnDbSuffix => UsePrecalculatedIndex ? Mmseqs2Internal.PrecalculatedIndexSuffix : Mmseqs2Internal.SourceDatabaseAlignmentSuffix;
    
    public string MmseqsBinaryPath { get; set; }

    public int ThreadsPerProcess { get; set; } = 1;
    

    public Dictionary<string, string> PossibleDbExtensions()
    {
        return new Dictionary<string, string>()
        {
            { "name", $"{Mmseqs2Internal.DbDataSuffix}" },
            { "name.index", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbIndexSuffix}" },
            { "name.dbtype", $"{Mmseqs2Internal.DbDataSuffix}{Mmseqs2Internal.DbTypeSuffix}" },
            { "name_h", $"{Mmseqs2Internal.DbHeaderSuffix}" },
            { "name_h.index", $"{Mmseqs2Internal.DbHeaderSuffix}{Mmseqs2Internal.DbIndexSuffix}" },
            { "name_h.dbtype", $"{Mmseqs2Internal.DbHeaderSuffix}{Mmseqs2Internal.DbTypeSuffix}" },
            { "name.lookup", $"{Mmseqs2Internal.DbLookupSuffix}" }
        };
    }

}