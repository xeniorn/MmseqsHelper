namespace MmseqsHelperLib;

public class ColabfoldMmseqsParametersConfiguration
{
    public ColabfoldMmseqsPairedParametersConfiguration Paired { get; init; }
    public string Search { get; init; }
    public ColabfoldMmseqsUnpairedParametersConfiguration Unpaired { get; init; }
}