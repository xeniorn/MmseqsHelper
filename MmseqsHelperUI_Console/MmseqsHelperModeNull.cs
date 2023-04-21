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
{string.Join(Environment.NewLine, Constants.AvailableModes.Select(x=>x.VerbString))}
###################
Example:
colabfold-search-mimic --InputFastaPaths=""input.fasta"" --MmseqsBinaryPath=""mmseqs"" --UniprotDbPath=""/resources/colabfold/db/uniref30_2202_db"" --EnvDbPath=/resources/colabfold/db/colabfold_envdb --OutputPath=/path/to/out --PersistedResultsPath=""/path/to/persisted"" --TempPath=/path/to/temp --UseRamPreloading=false --UseEnv=true --UsePairing=true --UsePrecalculatedIndex=true --ThreadsPerMmseqsProcess=1 --DeleteTemporaryData=true
###################"
            ;
    }
}

internal static class Constants
{
    public static List<MmseqsHelperMode> AvailableModes = new List<MmseqsHelperMode>()
    {
        new MmseqsHelperModeGenerateMonoDbs("auto-mono"),
        new MmseqsHelperModeGenerateA3mFilesForColabfold("auto-pair"),
        new MmseqsHelperModeColabfoldSearchMimic("colabfold-search-mimic"),
    };
}