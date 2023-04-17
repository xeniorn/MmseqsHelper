namespace MmseqsHelperLib;

public class PersistedMonoDatabaseConfiguration
{
    public string ForPairingAlignDbName { get; init; } = @"for_pairing_align";

    public string InfoFilename { get; set; } = @"database_info.json";
    
    /// <summary>
    /// database info file 1, qdb(seq, h)*(data, index, dbtype) 6
    /// </summary>
    public int MinimalNumberOfFilesInResultFolder { get; init; } = 7;

    public string MonoA3mDbName { get; init; } = @"mono_a3m";
    public string QdbName { get; init; } = @"qdb";
    public string LastAccessReporterFilename { get; init; } = "LAST_USED";
}