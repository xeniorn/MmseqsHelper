namespace MmseqsHelperUI_Console;

internal class MmseqsHelperModeNull : MmseqsHelperMode
{
    public MmseqsHelperModeNull()
    {
        VerbString = String.Empty;
        Process = MmseqsAutoProcess.Null;
    }

    public override Dictionary<string, (string defaultValue, bool required)> GetDefaults()
    {
        return new Dictionary<string, (string defaultValue, bool required)>();
    }

    public override string GetHelpString(string envVarPrefix, string defaultConfigName)
    {
        return
            @$"###### Mmseqs helper                   
Usage:
MmseqsHelperUI_Console COMMAND [INPUTS]
###################
Each option can be provided via appsettings.json, environmental variables (with added prefix {envVarPrefix}), or the command line. Later sources will override earlier sources.
Options are case-insensitive and can be prefixed with '/' '--' (recommended) or nothing. Equal sign for each option is recommended but optional unless no prefix is used.
###################
Commands:
###################
auto-mono
auto-pair
###################";
    }
}