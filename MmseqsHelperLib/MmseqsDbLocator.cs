using AlphafoldPredictionLib;

namespace MmseqsHelperLib;

internal class MmseqsDbLocator
{
    public string QdbPath { get; } = string.Empty;
    public Dictionary<MmseqsSourceDatabaseTarget, (string unpairedDbPath, string pairedDbPath)> DatabasePathMapping { get; } = new();
    public Dictionary<PredictionTarget, List<int>> QdbIndicesMapping { get; } = new ();
}