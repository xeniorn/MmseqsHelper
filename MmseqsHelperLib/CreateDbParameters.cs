namespace MmseqsHelperLib;

public class CreateDbParameters : MmseqsCommandLineParameters
{
    public CreateDbParameters()
    {
        var defaultParams = new List<ICommandLineParameter>()
        {
            new CommandLineParameter<int>("--dbtype", "Database type 0: auto, 1: amino acid 2: nucleotides", 0, new List<CommandLineFeature>() 
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
            new CommandLineParameter<bool>("--shuffle", "Shuffle input database", true, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
            new CommandLineParameter<int>("--createdb-mode", "Createdb mode 0: copy data, 1: soft link data and write new index (works only with single line fasta/q)", 0, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),                
            new CommandLineParameter<int>("--id-offset", "Numeric ids in index file are offset by this value", 0, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
            new CommandLineParameter<int>("--compressed", "Write compressed output [0]", 0, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace}),
            new CommandLineParameter<int>("-v", "Verbosity level: 0: quiet, 1: +errors, 2: +warnings, 3: +info", 3, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace, CommandLineFeature.AffectsOnlyVerbosityOrSimilar}),
            new CommandLineParameter<int>("--write-lookup", "write .lookup file containing mapping from internal id, fasta id and file number [1]", 1, new List<CommandLineFeature>()
                {CommandLineFeature.MustHaveValue, CommandLineFeature.CanAcceptValueAfterWhiteSpace})
        };

        Parameters = defaultParams;
    }

    public override string CommandString => "createdb";
}