namespace MmseqsHelperUI_Console;

internal static class Constants
{
    public static List<MmseqsHelperMode> AvailableModes = new List<MmseqsHelperMode>()
    {
        new MmseqsHelperModeGenerateMonoDbs("auto-mono"),
        new MmseqsHelperModeGenerateA3mFilesForColabfold("auto-pair"),
        new MmseqsHelperModeColabfoldSearchMimic("colabfold-search-mimic"),
    };
}