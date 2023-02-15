namespace MmseqsHelperLib
{
    internal interface ICommandLineParameter
    {
        public Type Type { get; }
        public string Flag { get; }
        public IComparable DefaultValue { get; }
        public List<CommandLineFeature> ParameterFeatures { get; }
        public string Description { get; }
        public IComparable Value { get; set; }

        string GetCommandLineString()
        {
            if (ParameterFeatures.Contains(CommandLineFeature.MustHaveValue))
            {
                var spacer = String.Empty;
                //prefer spacers in this order
                if (ParameterFeatures.Contains(CommandLineFeature.CanAcceptValueWithoutSpacer)) spacer = "";
                else if (ParameterFeatures.Contains(CommandLineFeature.CanAcceptValueAfterWhiteSpace)) spacer = " ";
                else if (ParameterFeatures.Contains(CommandLineFeature.CanAcceptValueAfterEqualsSign)) spacer = "=";
                else throw new Exception("Invalid combination of Parameter Features");
                return $"{Flag}{spacer}{Value.ToString()}";
            }
            
            return $"{Flag}";

        }

        private static string EnsureQuotedIfWhiteSpace(string input)
        {
            if (!HasWhitespace(input)) return input;
            return IsQuoted(input) ? input : $"\"{input}\"";
        }
        private static bool HasWhitespace(string input) => input.Any(x => Char.IsWhiteSpace(x));
        private static bool IsQuoted(string input) => input.Length > 2 && input.First() == '"' && input.Last() == '"';


    }
}