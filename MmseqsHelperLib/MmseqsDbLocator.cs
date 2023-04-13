﻿using AlphafoldPredictionLib;

namespace MmseqsHelperLib;

internal class MmseqsDbLocator
{
    public string QdbPath { get; set; } = string.Empty;
    public Dictionary<MmseqsSourceDatabaseTarget, string> UnPairedA3mDbPathMapping { get; } = new();
    public Dictionary<MmseqsSourceDatabaseTarget, string> PairedA3mDbPathMapping { get; } = new();
    public Dictionary<MmseqsSourceDatabaseTarget, string> PrePairingAlignDbPathMapping { get; } = new();
    //public Dictionary<MmseqsSourceDatabaseTarget, (string unpairedDbPath, string pairedDbPath)> DatabasePathMapping { get; } = new();
    public Dictionary<PredictionTarget, List<int>> QdbIndicesMapping { get; set; } = new ();
}