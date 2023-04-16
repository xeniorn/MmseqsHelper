using System.Text.Json;

namespace MmseqsHelperLib;

internal class PersistedMonoDbMetadataInfo
{
    public PersistedMonoDbMetadataInfo()
    {
    }

    private static JsonSerializerOptions _jsonSerializerOptions = new()
    {
        IgnoreReadOnlyFields = true,
        IgnoreReadOnlyProperties = true,
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
        await System.Text.Json.JsonSerializer.SerializeAsync<PersistedMonoDbMetadataInfo>(stream, this, _jsonSerializerOptions);
    }

}