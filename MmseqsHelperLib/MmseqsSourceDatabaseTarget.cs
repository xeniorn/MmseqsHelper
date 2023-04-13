namespace MmseqsHelperLib;

public class MmseqsSourceDatabaseTarget
{
    public MmseqsSourceDatabaseTarget(MmseqsSourceDatabase db, bool useForMono, bool useForPaired)
    {
        Database = db;
        UseForMono = useForMono;
        UseForPaired = useForPaired;
    }

    public MmseqsSourceDatabase Database { get; }

    public bool UseForMono { get; }

    public bool UseForPaired { get; }
}