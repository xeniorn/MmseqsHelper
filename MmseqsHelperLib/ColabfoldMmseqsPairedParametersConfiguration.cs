namespace MmseqsHelperLib;

public class ColabfoldMmseqsPairedParametersConfiguration
{
    public string Align1 { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_Align1ParamsPair;
    public string Align2 { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_Align2ParamsPair;
    public string Expand { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_ExpandParamsUnirefPair;
    public string MsaConvert { get; set; } = ColabfoldMmseqsHelperSettings.colabFold_MsaConvertParamsPair;

}