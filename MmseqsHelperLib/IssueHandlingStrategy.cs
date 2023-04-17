using System.Runtime.InteropServices.ObjectiveC;
using Microsoft.Extensions.Logging;

namespace MmseqsHelperLib;

public class IssueHandlingStrategy
{
    public SuspiciousDataStrategy SuspiciousData { get; set; }
    public bool AllowDifferentMmseqsVersion { get; set; }

    public IssueHandlingPolicy MmseqsVersionOfPersistedDatabaseIsLowerThanRunningVersion { get; } =
        new IssueHandlingPolicy()
        {
            ActionsRequsted = new List<IssueHandlingAction>()
            {
                IssueHandlingAction.Report
            }
        };
}

/// <summary>
/// Actions are _requested_. It is not guaranteed that the target will conform to it, it signals intent by the higher level program, not insurance.
/// </summary>
public class IssueHandlingPolicy
{
    public List<IssueHandlingAction> ActionsRequsted { get; init; } = new();
    public Dictionary<string,object>? AdditionalSettings;

    public T GetValueOrDefault<T>(string key, T defaultValue)
    {
        if (AdditionalSettings is null) return defaultValue;
        if (!AdditionalSettings.ContainsKey(key)) return defaultValue;
        
        var value = AdditionalSettings[key];
        if (value is not T) return defaultValue;
        
        return (T)value;
    }
}

public enum IssueHandlingAction
{
    Report,
    SkipCurrentItem,
    StopProcessing,
    KillProgram
}