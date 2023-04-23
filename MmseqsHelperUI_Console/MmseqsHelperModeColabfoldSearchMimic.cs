namespace MmseqsHelperUI_Console;

internal class MmseqsHelperModeColabfoldSearchMimic : MmseqsHelperMode
{
    public MmseqsHelperModeColabfoldSearchMimic(string cliVerbString)
    {
        VerbString = cliVerbString;
        Process = MmseqsAutoProcess.MimicColabfoldSearch;
    }

    public override Dictionary<string, (string defaultValue, bool required, string description)> GetDefaults()
    {
        return new Dictionary<string, (string defaultValue, bool required, string description)>()
        {
            { "InputFastaPaths", ("\"fasta1,fasta2,fasta3\"", true, @"comma-separated list of paths from which inputs will be read") },
            { "PersistedResultsPath", ("/path/to/persisted/db/", true, @"path to where the persisted result db is located (where the results will also be appended to)") },
            { "MmseqsBinaryPath", ("mmseqs", true, @"path to the mmseqs binary, without any parameters") },
            { "UniprotDbPath", ("/resources/colabfold/dbs/uniref30_2202_db", true, @"full path to the uniprot source db") },
            { "EnvDbPath", ("/resources/colabfold/dbs/colabfold_envdb_202108_db", true, @"full path to the metagenomic (env) source db") },
            { "OutputPath", ("./output/", true, @"path where the colabfold-style outputs will be copied to") },
            { "TempPath", ("./tmp/", false, @"path to the temp directory")},
            {"ThreadsPerMmseqsProcess", ("1",false, @"number of threads that will be used for _each_ mmseqs process - in addition, separate mmseqs processes will perform processing for each input db in parallel") },
            {"PreLoadDb", ("false",false, @"whether the database is preloaded into memory (false/f/0 or true/t/1)") },
            {"UsePrecalculatedIndex",("true",false, @"whether the database is preloaded into memory (false/f/0 or true/t/1)") },
            {"UseEnv",("true",false, @"whether the metagenomic db should be used (false/f/0 or true/t/1)") },
            {"UsePair",("true",false, @"whether the taxonomic pairing should be used for complexes (false/f/0 or true/t/1)") },
            {"PairingMaxBatchSize",("1000",false, @"max batch size for MSA pairing") },
            {"SearchMaxBatchSize",("500",false, @"max batch size for initial mmseqs search generating monomeric results") },
            {"ExistingDbSearchParallelization",("20",false, @"how many locations should be read in parallel when searching for existing results ") },
            {"DeleteTemporaryData", ("true", false, @"whether the temp data should be deleted in the end (false/f/0 or true/t/1)") }

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
 {string.Join(Environment.NewLine,
     defaults.Select(x =>
         $"{x.Key}\t{(x.Value.required ? String.Empty : $" [optional]:\t{x.Value.description}")}\t(DEFAULT: {x.Value.defaultValue})")) }        
###################";
    }
}
