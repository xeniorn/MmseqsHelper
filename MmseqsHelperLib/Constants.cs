namespace MmseqsHelperLib;

public static class Constants
{
    public static readonly Dictionary<SupportedMmseqsModule,string> ModuleStrings = new Dictionary<SupportedMmseqsModule,string>()
    {
        {SupportedMmseqsModule.Search, "search" },
        {SupportedMmseqsModule.ExpandAlignment, "expandaln" },
        { SupportedMmseqsModule.Align, "align"},
        {SupportedMmseqsModule.FilterResult, "filterresult" },
        { SupportedMmseqsModule.ConvertResultToMsa , "result2msa"},
        {SupportedMmseqsModule.LinkDatabase, "lndb" },
        {SupportedMmseqsModule.MoveDatabase, "mvdb" },
        {SupportedMmseqsModule.MergeDatabases, "mergedbs" }
    };

}