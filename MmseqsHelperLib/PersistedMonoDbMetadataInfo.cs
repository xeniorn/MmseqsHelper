using System.Text.Json;

namespace MmseqsHelperLib;

internal class PersistedMonoDbMetadataInfo
{

    private static JsonSerializerOptions _jsonSerializerOptions = new()
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

    public PersistedMonoDbMetadataInfo(DateTime createTime, MmseqsSourceDatabaseTarget referenceDbTarget, List<MmseqsSourceDatabaseTarget> databaseTargets, string mmseqsHelperDatabaseVersion, int targetCount, string mmseqsVersion)
    {
        CreateTime = createTime;
        ReferenceDbTarget = referenceDbTarget;
        DatabaseTargets = databaseTargets;
        MmseqsHelperDatabaseVersion = mmseqsHelperDatabaseVersion;
        TargetCount = targetCount;
        MmseqsVersion = mmseqsVersion;
    }

    public string MmseqsHelperDatabaseVersion { get; set; }
    public DateTime CreateTime { get; set; }
    public string MmseqsVersion { get; set; }
    public int TargetCount { get; set; }
    public MmseqsSourceDatabaseTarget ReferenceDbTarget { get; set; }
    public List<MmseqsSourceDatabaseTarget> DatabaseTargets { get; set; } = new();

    public async Task WriteToFileSystemAsync(string fullInfoPath)
    {
        await using var stream = File.Create(fullInfoPath);
        await JsonSerializer.SerializeAsync<PersistedMonoDbMetadataInfo>(stream, this, _jsonSerializerOptions);
    }

    public static async Task<PersistedMonoDbMetadataInfo?> ReadFromFileSystemAsync(string fullInfoFilePath)
    {
        await using var stream = File.OpenRead(fullInfoFilePath);

        try
        {
            return await JsonSerializer.DeserializeAsync<PersistedMonoDbMetadataInfo>(stream, _jsonSerializerOptions);
        }
        catch (Exception ex)
        {
            return null;
        }
    }

}