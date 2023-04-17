namespace MmseqsHelperLib;

public class ColabfoldMmseqsUnpairedParametersConfiguration
{
    public string Align { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_AlignParamsMono;
    public string Expand { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_ExpandParamsEnvMono;
    public string Filter { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_FilterParams;
    public string MsaConvert { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_MsaConvertParamsMono;
}