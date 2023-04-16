namespace MmseqsHelperLib;

public class MmseqsSourceDatabaseTarget
{
    public MmseqsSourceDatabaseTarget(MmseqsSourceDatabase database, bool useForUnpaired, bool useForPaired)
    {
        Database = database;
        UseForUnpaired = useForUnpaired;
        UseForPaired = useForPaired;
    }

    public MmseqsSourceDatabase Database { get; }

    public bool UseForUnpaired { get; }

    public bool UseForPaired { get; }
}