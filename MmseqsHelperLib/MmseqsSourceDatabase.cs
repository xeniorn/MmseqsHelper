namespace MmseqsHelperLib;

public class MmseqsSourceDatabase
{
    public MmseqsSourceDatabase(string dbName, string dbPath, MmseqsSourceDatabaseFeatures features)
    {
        this.Name = dbName;
        this.Path = dbPath;
        this.Features = features;
    }

    public MmseqsSourceDatabaseFeatures Features { get; }

    public string Name { get; }
    public string Path { get; }
}