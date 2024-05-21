using System.Net.NetworkInformation;

namespace MmseqsHelperLib;

public class ColabfoldHelperComputationInstanceInfo
{
    public string HelperDatabaseVersion { get; set; }
    public string? MmseqsVersion { get; set; }
    public string ComputerIdentifier { get; set; }
    
    public void InitalizeFromSettings(ColabfoldMmseqsHelperSettings settings)
    {
        ComputerIdentifier = GetComputerId(settings.ComputingConfig.TrackingConfig);
    }

    private string GetComputerId(TrackingStrategyConfiguration trackingStrategy)
    {
        string identifier;

        try
        {
            switch (trackingStrategy.ComputerIdentifierSource)
            {
                case TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.None:
                    identifier = string.Empty; break;
                case TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.FirstNetworkInterface:
                    var ni = NetworkInterface.GetAllNetworkInterfaces().First();
                    var niId = ni.Id;
                    identifier = niId.ToString();
                    break;
                case TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.HostName:
                    var ipProperties = IPGlobalProperties.GetIPGlobalProperties();
                    var fqdn = string.Format("{0}{1}", ipProperties.HostName, string.IsNullOrWhiteSpace(ipProperties.DomainName) ? "" : $".{ipProperties.DomainName}" );
                    identifier = fqdn;
                    ;
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
        catch (Exception ex)
        {
            //_logger.LogInformation("Failed to get computer identification based on strategy, will use empty identifier.");
            trackingStrategy.ComputerIdentifierSource = TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.None;
            identifier = string.Empty;
        }

        return identifier;
    }
}