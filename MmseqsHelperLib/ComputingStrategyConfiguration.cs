namespace MmseqsHelperLib;

public class ComputingStrategyConfiguration
{
    public int ExistingDatabaseSearchParallelizationFactor { get; set; }
    public int MaxDesiredMonoBatchSize { get; set; }
    public int MaxDesiredPredictionTargetBatchSize { get; set; }
    
    public string TempPath { get; set; }
    
    public MmseqsSettings MmseqsSettings { get; set; }
    public bool ReportSuccessfulUsageOfPersistedDb { get; set; }

    public bool DeleteTemporaryData { get; set; } = true;
    public TrackingStrategyConfiguration TrackingConfig { get; set; }
}



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