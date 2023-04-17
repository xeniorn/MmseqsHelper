using System.Text.Json;

namespace MmseqsHelperLib;

public class ColabfoldMmseqsHelperSettings
{
    public const string colabFold_Align1ParamsPair = @"-e 0.001  --max-accept 1000000 -c 0.5 --cov-mode 1";
    public const string colabFold_Align2ParamsPair = @"-e inf";
    public const string colabFold_AlignParamsMono = @"-e 10  --max-accept 1000000 --alt-ali 10 -a";
    public const string colabFold_ExpandParamsEnvMono = @"--expansion-mode 0 -e inf";
    public const string colabFold_ExpandParamsUnirefMono = @"--expansion-mode 0 -e inf --expand-filter-clusters 1 --max-seq-id 0.95";
    public const string colabFold_ExpandParamsUnirefPair = @"--expansion-mode 0 -e inf --expand-filter-clusters 0 --max-seq-id 0.95";
    public const string colabFold_FilterParams = @"--qid 0 --qsc 0.8 --diff 0 --max-seq-id 1.0 --filter-min-enable 100";
    public const string colabFold_MsaConvertParamsMono = @"--msa-format-mode 6 --filter-msa 1 --filter-min-enable 1000 --diff 3000 --qid '0.0,0.2,0.4,0.6,0.8,1.0' --qsc 0 --max-seq-id 0.95";
    public const string colabFold_MsaConvertParamsPair = @"--msa-format-mode 5";
    public const string colabFold_SearchParamsShared = @"--num-iterations 3 -a -s 8 -e 0.1 --max-seqs 10000";

    public ColabfoldMmseqsHelperSettings(ColabfoldMmseqsParametersConfiguration colabfoldMmseqsParams,
        ColabfoldMmseqsUnpairedParametersConfiguration colabfoldMmseqsParamsUnpairedSpecialForReferenceDb,
        PersistedA3mDatabaseConfiguration persistedA3MDbConfig,
        PersistedMonoDatabaseConfiguration persistedMonoDbConfig, SearchDatabasesConfiguration searchDatabasesConfig,
        IssueHandlingStrategy strategy, ComputingStrategyConfiguration computingConfig)
    {
        ColabfoldMmseqsParams = colabfoldMmseqsParams;
        ColabfoldMmseqsParamsUnpairedSpecialForReferenceDb = colabfoldMmseqsParamsUnpairedSpecialForReferenceDb;
        PersistedA3mDbConfig = persistedA3MDbConfig;
        PersistedMonoDbConfig = persistedMonoDbConfig;
        SearchDatabasesConfig = searchDatabasesConfig;
        ComputingConfig = computingConfig;
        Strategy = strategy;
    }


    public string ColabfoldComplexFastaMonomerSeparator { get; init; } = ":";
    

        public ColabfoldMmseqsParametersConfiguration ColabfoldMmseqsParams { get; init; }
    

    public ColabfoldMmseqsUnpairedParametersConfiguration ColabfoldMmseqsParamsUnpairedSpecialForReferenceDb { get; init; } 


    public ComputingStrategyConfiguration ComputingConfig { get; init; }

    //public Dictionary<string, string> Custom { get; init; } = new();
    
    public PersistedA3mDatabaseConfiguration PersistedA3mDbConfig { get; init; }
    public PersistedMonoDatabaseConfiguration PersistedMonoDbConfig { get; init; }
    public SearchDatabasesConfiguration SearchDatabasesConfig { get; init; }
    
    public IssueHandlingStrategy Strategy { get; init; }

    public string ToJson()
    {
        var a = JsonSerializer.Serialize(this, new JsonSerializerOptions()
        {
            WriteIndented = true,
            IgnoreReadOnlyFields = false,
            IgnoreReadOnlyProperties = false,
            AllowTrailingCommas = true,
        });

        return a;
    }
}