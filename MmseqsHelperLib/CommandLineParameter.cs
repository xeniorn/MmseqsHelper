namespace MmseqsHelperLib
{
    internal class CommandLineParameter<T> : ICommandLineParameter where T : IComparable
    {
        public CommandLineParameter(string flag, string description, T defaultValue) : this(flag, description, defaultValue, new List<CommandLineFeature>())
        {
        }

        public CommandLineParameter(string flag, string description, T defaultValue, IEnumerable<CommandLineFeature> features)
        {
            Flag = flag;
            Description = description;
            DefaultValue = defaultValue;

            ParameterFeatures.AddRange(features.Distinct());

        }

        public Type Type => typeof(T);
        public string Flag { get; }
        public IComparable DefaultValue { get; }
        public T DefaultValueAsType => (T)DefaultValue;
        public List<CommandLineFeature> ParameterFeatures {get;} = new List<CommandLineFeature>();
        public string Description { get; }
        public IComparable Value { get; set; }
    }

    public enum CommandLineFeature
    {
        MustHaveValue,
        CanAcceptValueAfterWhiteSpace,
        CanAcceptValueAfterEqualsSign,
        Mandatory,
        AffectsOnlyVerbosityOrSimilar,
        CanAcceptValueWithoutSpacer
    }
}