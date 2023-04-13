using System.Text.Json;

namespace MmseqsHelperLib;

internal class PersistedMonoDbMetadataInfo
{
    public string MmseqsHelperVersion { get; set; }
    public DateTime CreateTime { get; set; }
    public string MmseqsVersion { get; set; }
    public int TargetCount { get; set; }
    public MmseqsSourceDatabaseTarget ReferenceDbTarget { get; set; }
    public List<MmseqsSourceDatabaseTarget> DatabaseTargets { get; set; }

    public async Task WriteToFileSystemAsync(string fullInfoPath)
    {
        await using var stream = File.Create(fullInfoPath);
        await System.Text.Json.JsonSerializer.SerializeAsync<PersistedMonoDbMetadataInfo>(stream, this, new JsonSerializerOptions(){AllowTrailingCommas = true, WriteIndented = true});
    }

}