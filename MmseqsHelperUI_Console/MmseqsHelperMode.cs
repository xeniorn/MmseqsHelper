namespace MmseqsHelperUI_Console;

internal abstract class MmseqsHelperMode
{
    public static MmseqsHelperMode Null => new MmseqsHelperModeNull();

    public string VerbString { get; init; }
    public MmseqsAutoProcess Process { get; init; }

    public abstract Dictionary<string, (string defaultValue, bool required, string description)> GetDefaults();
    public abstract string GetHelpString(string envVarPrefix, string defaultConfigName);

    public override string ToString()
    {
        return $"{VerbString} / {Enum.GetName(typeof(MmseqsAutoProcess), Process)}";
    }
}