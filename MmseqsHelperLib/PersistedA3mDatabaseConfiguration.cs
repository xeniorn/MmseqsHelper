namespace MmseqsHelperLib;

public class PersistedA3mDatabaseConfiguration
{
    public string A3mInfoFilename { get; init; } = @"msa_info.json";
    /// <summary>
    /// for organization of final a3m files into subfolders, taking progressively larger substrings from left
    /// </summary>
    public List<int> FolderOrganizationFragmentLengths { get; set; } = new() { 2, 4 };

    public string ResultA3mFilename { get; init; } = @"msa.a3m";
    /// <summary>
    /// subset of MD5 hash to use as a3m prediction id
    /// </summary>
    public int ShortBatchIdLength { get; init; } = 8;
}