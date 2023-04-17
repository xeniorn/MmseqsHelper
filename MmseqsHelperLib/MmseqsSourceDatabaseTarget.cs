namespace MmseqsHelperLib;

public class MmseqsSourceDatabaseTarget
{
    public MmseqsSourceDatabaseTarget(MmseqsSourceDatabase database, bool useForUnpaired, bool useForPaired, bool requestPreloadingToRam)
    {
        Database = database;
        UseForUnpaired = useForUnpaired;
        UseForPaired = useForPaired;
        RequestPreloadingToRam = requestPreloadingToRam;
    }

    public MmseqsSourceDatabase Database { get; }

    public bool UseForUnpaired { get; }
    public bool UseForPaired { get; }
    public bool RequestPreloadingToRam { get; }

}