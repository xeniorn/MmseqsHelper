namespace MmseqsHelperLib;

public class TrackingStrategyConfiguration
{
    public enum ComputerIdentifierSourceStrategy
    {
        None,
        FirstNetworkInterface,
        HostName
    }

    const ComputerIdentifierSourceStrategy DefaultComputerIdentificationSourceStrategy = ComputerIdentifierSourceStrategy.FirstNetworkInterface;
    public ComputerIdentifierSourceStrategy ComputerIdentifierSource { get; set; } = DefaultComputerIdentificationSourceStrategy;
    
}