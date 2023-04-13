using System.Text.Json;
using AlphafoldPredictionLib;

namespace MmseqsHelperLib;

public class ColabfoldMsaMetadataInfo
{
    public string MmseqsHelperVersion { get; set; }
    public DateTime CreateTime { get; set; }
    public List<MsaOriginDefinition> MsaOriginDefinitions { get; set; } = new ();
    public PredictionTarget PredictionTarget { get; set; }

    public async Task WriteToFileSystemAsync(string fullInfoPath)
    {
        await using var stream = File.Create(fullInfoPath);
        await System.Text.Json.JsonSerializer.SerializeAsync<ColabfoldMsaMetadataInfo>(stream, this, new JsonSerializerOptions(){ AllowTrailingCommas = true, WriteIndented = true});
    }
}