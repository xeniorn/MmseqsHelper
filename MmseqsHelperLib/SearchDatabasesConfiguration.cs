namespace MmseqsHelperLib;

public class SearchDatabasesConfiguration
{
    /// <summary>
    /// Each dbTarget is a combination of a database and an intent do generate paired or unpaired reads, etc
    /// </summary>
    public List<MmseqsSourceDatabaseTarget> DbTargets { get; set; }

    /// <summary>
    /// The one with which the first search is done. The rest of searches is done using a profile result from this search.
    /// </summary>
    public MmseqsSourceDatabaseTarget ReferenceDbTarget { get; set; }
}