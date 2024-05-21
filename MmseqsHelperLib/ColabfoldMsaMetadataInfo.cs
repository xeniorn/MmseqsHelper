using System.Text.Json;

namespace MmseqsHelperLib;

public class ColabfoldMsaMetadataInfo
{
    private static JsonSerializerOptions _jsonSerializerOptions = new ()
    {
        IgnoreReadOnlyFields = true,
        // need to leave this at false because otherwise the DatabaseTarget (a record) doesn't export the Database...
        // Could handle it separately, but I don't know where else it will be an issue. Safer to keep it.
        // It doesn't hurt anything except clutter.
        IgnoreReadOnlyProperties = false,
        PropertyNameCaseInsensitive = true,
        AllowTrailingCommas = true, 
        WriteIndented = true
    };

    public ColabfoldMsaMetadataInfo()
    {

    }

    public ColabfoldMsaMetadataInfo(ColabfoldPredictionTarget predictionTarget, DateTime createTime,
         ColabfoldMmseqsHelperSettings settings,
        ColabfoldHelperComputationInstanceInfo computationInstanceInfo)
    {
        PredictionTarget = predictionTarget;
        CreateTime = createTime;
        MmseqsHelperDatabaseVersion = computationInstanceInfo.HelperDatabaseVersion ?? String.Empty;
        MmseqsVersion = computationInstanceInfo.MmseqsVersion ?? String.Empty;

        ComputationInfoReport = new ComputationInfoReport(settings, computationInstanceInfo);
    }
    

    public string MmseqsHelperDatabaseVersion { get; set; }
    public string MmseqsVersion { get; set; }
    public ComputationInfoReport ComputationInfoReport { get; private set; } = new ComputationInfoReport();
    public DateTime CreateTime { get; set; }
    public List<MsaOriginDefinition> MsaOriginDefinitions { get; set; } = new ();
    public ColabfoldPredictionTarget PredictionTarget { get; set; }

    public async Task WriteToFileSystemAsync(string fullInfoPath)
    {
        await using var stream = File.Create(fullInfoPath);
        await JsonSerializer.SerializeAsync<ColabfoldMsaMetadataInfo>(stream, this, _jsonSerializerOptions);
    }

    public static async Task<ColabfoldMsaMetadataInfo?> ReadFromFileSystemAsync(string fullInfoFilePath)
    {
        await using var stream = File.OpenRead(fullInfoFilePath);

        try
        {
            return await JsonSerializer.DeserializeAsync<ColabfoldMsaMetadataInfo>(stream, _jsonSerializerOptions);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
}