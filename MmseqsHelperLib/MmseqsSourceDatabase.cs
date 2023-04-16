namespace MmseqsHelperLib;

public class MmseqsSourceDatabase
{
    public MmseqsSourceDatabase(string name, string path, MmseqsSourceDatabaseFeatures features)
    {
        this.Name = name;
        this.Path = path;
        this.Features = features;
    }

    public MmseqsSourceDatabaseFeatures Features { get; }

    public string Name { get; }
    public string Path { get; }
}