namespace MmseqsHelperLib;

public class IssueHandlingStrategy
{
    public SuspiciousDataStrategy SuspiciousData { get; set; }
    public bool AllowDifferentMmseqsVersion { get; set; }

    public IssueHandlingPolicy MmseqsVersionOfPersistedDatabaseIsLowerThanRunningVersion { get; } = new ()
        {
            ActionsRequsted = new ()
            {
                IssueHandlingAction.Report
            }
        };

    public IssueHandlingPolicy ProcessingBatchFailedCatastrophically { get; } = new()
    {
        ActionsRequsted = new ()
        {
            IssueHandlingAction.Report,
            IssueHandlingAction.SkipCurrentItem
        }
    };

    public IssueHandlingPolicy FinalMsaGenerationFailedForSinglePrediction { get; } = new()
    {
        ActionsRequsted = new()
        {
            IssueHandlingAction.Report,
            IssueHandlingAction.SkipCurrentItem
        }
    };
}