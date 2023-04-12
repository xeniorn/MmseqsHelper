namespace MmseqsHelperLib;

public class ColabfoldMsaMetadataInfo
{
    public DateTime CreateTime { get; set; }
    public List<MsaOriginDefinition> MsaOriginDefinitions { get; set; } = new List<MsaOriginDefinition>();

    public async Task WriteToFileSystemAsync(string fullInfoPath)
    {
        await using var stream = File.Create(fullInfoPath);
        await System.Text.Json.JsonSerializer.SerializeAsync<ColabfoldMsaMetadataInfo>(stream, this);
    }
}