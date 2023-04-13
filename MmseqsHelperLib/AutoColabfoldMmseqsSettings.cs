namespace MmseqsHelperLib;

public class AutoColabfoldMmseqsSettings
{
    public int PersistedA3mDbShortBatchIdLength { get; init; } = 8; // subset of MD5 hash to use as a3m prediction id
    public string PersistedMonoDbInfoName { get; set; } = @"database_info.json";
    public string PersistedDbFinalA3mInfoName { get; init; } = @"msa_info.json";
    public string PersistedDbFinalA3mName { get; init; } = @"msa.a3m";
    public string PersistedDbQdbName { get; init; } = @"qdb";
    public string PersistedDbPairModeFirstAlignDbName { get; init; } = @"for_pairing_align_mmseqsdb";
    public string PersistedDbMonoModeResultDbName { get; init; } = @"final_mono_a3m_mmseqsdb";
    
    public string TempPath { get; set; } = Path.GetTempPath();
    
    public string FastaSuffix { get; init; } = @".fasta";
    public Dictionary<string, string> Custom { get; init; } = new();

    public int MaxDesiredMonoBatchSize { get; init; } = 500;
    
    public int MaxDesiredPredictionTargetBatchSize { get; set; } = 200;
    // database info file 1, qdb(seq, h)*(data, index, dbtype) 6
    public int PersistedDbMinimalNumberOfFilesInMonoDbResult { get; init; } = 7;
    
    // for organization of final a3m files into subfolders
    public string ColabfoldComplexFastaMonomerSeparator { get; init; } = ":";

    public string ColabFold_Align1ParamsPair { get; init; } = colabFold_Align1ParamsPair;
    public string ColabFold_Align2ParamsPair { get; init; } = colabFold_Align2ParamsPair;
    public string ColabFold_AlignParamsMono { get; init; } = colabFold_AlignParamsMono;
    public string ColabFold_ExpandParamsEnvMono { get; init; } = colabFold_ExpandParamsEnvMono;
    public string ColabFold_ExpandParamsUnirefMono { get; init; } = colabFold_ExpandParamsUnirefMono;
    public string ColabFold_ExpandParamsUnirefPair { get; init; } = colabFold_ExpandParamsUnirefPair;
    public string ColabFold_FilterParams { get; init; } = colabFold_FilterParams;
    public string ColabFold_MsaConvertParamsMono { get; init; } = colabFold_MsaConvertParamsMono;
    public string ColabFold_MsaConvertParamsPair { get; init; } = colabFold_MsaConvertParamsPair;
    public string ColabFold_SearchParamsShared { get; init; } = colabFold_SearchParamsShared;
    public List<int> PersistedA3mDbFolderOrganizationFragmentLengths { get; set; } = new List<int>() { 2, 4 };

    const string colabFold_Align1ParamsPair = @"-e 0.001  --max-accept 1000000 -c 0.5 --cov-mode 1";
    const string colabFold_Align2ParamsPair = @"-e inf";
    const string colabFold_AlignParamsMono = @"-e 10  --max-accept 1000000 --alt-ali 10 -a";
    const string colabFold_ExpandParamsEnvMono = @"--expansion-mode 0 -e inf";
    const string colabFold_ExpandParamsUnirefMono = @"--expansion-mode 0 -e inf --expand-filter-clusters 1 --max-seq-id 0.95";
    const string colabFold_ExpandParamsUnirefPair = @"--expansion-mode 0 -e inf --expand-filter-clusters 0 --max-seq-id 0.95";
    const string colabFold_FilterParams = @"--qid 0 --qsc 0.8 --diff 0 --max-seq-id 1.0 --filter-min-enable 100";
    const string colabFold_MsaConvertParamsMono = @"--msa-format-mode 6 --filter-msa 1 --filter-min-enable 1000 --diff 3000 --qid '0.0,0.2,0.4,0.6,0.8,1.0' --qsc 0 --max-seq-id 0.95";
    const string colabFold_MsaConvertParamsPair = @"--msa-format-mode 5";
    const string colabFold_SearchParamsShared = @"--num-iterations 3 -a -s 8 -e 0.1 --max-seqs 10000";

    public CalculationStrategy Strategy { get; init; } = new() {SuspiciousData = SuspiciousDataStrategy.PlaySafe};
    public string ColabfoldMmseqsHelperVersion { get; set; }
}