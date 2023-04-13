namespace MmseqsHelperLib;

public class MmseqsSourceDatabaseTarget
{
    public MmseqsSourceDatabaseTarget(MmseqsSourceDatabase db, bool useForUnpaired, bool useForPaired)
    {
        Database = db;
        UseForUnpaired = useForUnpaired;
        UseForPaired = useForPaired;
    }

    public MmseqsSourceDatabase Database { get; }

    public bool UseForUnpaired { get; }

    public bool UseForPaired { get; }
}