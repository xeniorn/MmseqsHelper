namespace MmseqsHelperLib;

internal class MmseqsQueryDatabaseContainer
{
    public MmseqsDatabaseObject DataDbObject { get; set; }
    public MmseqsDatabaseObject HeaderDbObject { get; set; }
    public MmseqsLookupObject LookupObject { get; set; }

    public async Task WriteToFileSystemAsync(MmseqsSettings settings, string dbPath)
    {
        var writeTasks = new List<Task>()
        {
            DataDbObject.WriteToFileSystemAsync(settings, dbPath),
            HeaderDbObject.WriteToFileSystemAsync(settings, dbPath),
            LookupObject.WriteToFileSystemAsync(settings, dbPath)
        };

        await Task.WhenAll(writeTasks);
    }
}