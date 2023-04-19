namespace MmseqsHelperUI_Console;

internal class MmseqsHelperModeColabfoldSearchMimic : MmseqsHelperMode
{
    public MmseqsHelperModeColabfoldSearchMimic(string cliVerbString)
    {
        VerbString = cliVerbString;
        Process = MmseqsAutoProcess.MimicColabfoldSearch;
    }

    public override Dictionary<string, (string defaultValue, bool required)> GetDefaults()
    {
        return new Dictionary<string, (string defaultValue, bool required)>()
        {
            { "InputFastaPaths", ("\"fasta1,fasta2,fasta3\"", true) },
            { "PersistedResultsPath", ("/path/to/persisted/db/", true) },
            { "MmseqsBinaryPath", ("./mmseqs", true) },
            { "UniprotDbPath", ("/resources/colabfold/UniRef30_2022_02/uniref30_2202_db", true) },
            { "EnvDbPath", ("/resources/colabfold/UniRef30_2022_02/colabfold_envdb_202108_db", true) },
            { "OutputPath", ("./output/", true) },
            { "TempPath", ("./tmp/", false)},
            {"Threads", ("1",false) },
            {"PreLoadDb", ("false",false)},
            {"UsePrecalculatedIndex",("true",false)},
            {"UseEnv",("true",false)},
            {"UsePair",("true",false)},
            {"PairingMaxBatchSize",("1000",false)},
            {"SearchMaxBatchSize",("500",false)},
            {"ExistingDbSearchParallelization",("20",false)}

        };
    }

    public override string GetHelpString(string envVarPrefix, string defaultConfigName)
    {
        var defaults = GetDefaults();

        return
            @$"###### Mmseqs helper                   
Usage:
MmseqsHelperUI_Console {VerbString} [INPUTS]
###################
Each option can be provided via appsettings.json, environmental variables (with added prefix {envVarPrefix}), or the command line. Later sources will override earlier sources.
Options are case-insensitive and can be prefixed with '/' '--' (recommended) or nothing. Equal sign for each option is recommended but optional unless no prefix is used.
###################
Options:
###################
"
            + string.Join("\n", defaults.Select(x => $"{x.Key} {x.Value.defaultValue}{(x.Value.required ? String.Empty : " [optional]")}"))
            + @"
###################";
    }
}