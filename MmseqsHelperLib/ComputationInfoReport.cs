namespace MmseqsHelperLib;

public class ComputationInfoReport
{
    public ComputationInfoReport()
    {
        
    }

    public Dictionary<string,string> Parameters { get; set; }

    public ComputationInfoReport(ColabfoldMmseqsHelperSettings settings,
        ColabfoldHelperComputationInstanceInfo instanceInfo)
    {
        InitStuffFromSettings(settings, instanceInfo);
    }

    private void InitStuffFromSettings(ColabfoldMmseqsHelperSettings settings, ColabfoldHelperComputationInstanceInfo instanceInfo)
    {
        Parameters = new Dictionary<string, string>
        {
            { "ComputerIdentifierSource", settings.ComputingConfig.TrackingConfig.ComputerIdentifierSource.ToString() },
            { "ComputerIdentifier", instanceInfo.ComputerIdentifier.ToString() },
        };
    }

}