using System.Text.Json;

namespace MmseqsHelperLib;

public class MmseqsInternalConfigurationSettings
{
    public const string ConfigurationName = "MmseqsInternal";

    public string DbHeaderSuffix { get; init; } = @"_h";
    public string DbDataSuffix { get; init; } = String.Empty;
    public string DbIndexSuffix { get; init; } = @".index";
    public string DbLookupSuffix { get; init; } = @".lookup";
    public string DbTypeSuffix { get; init; } = @".dbtype";

    public string SourceDatabaseSequenceSuffix { get; init; } = "_seq";
    public string SourceDatabaseAlignmentSuffix { get; init; } = "_aln";
    public string PrecalculatedIndexSuffix { get; init; } = ".idx";

    // manual says only \0 is the separator and zero-length entries indeed don't have a newline
    public string DataEntryTerminator { get; init; } = "\0";
    // newlines separate each index entry
    public string IndexEntryTerminator { get; init; } = "\n";
    // newlines separate each lookup entry
    public string LookupEntryTerminator { get; init; } = "\n";
    // headers and sequences have hardcoded newlines as a suffix
    public string HeaderEntryHardcodedSuffix { get; init; } = "\n";
    // headers and sequences have hardcoded newlines as a suffix
    public string SequenceEntryHardcodedSuffix { get; init; } = "\n";

    // within each entry, tabs are used, e.g.
    // 0    0   15
    // 1    15  15
    public string IndexIntraEntryColumnSeparator { get; init; } = "\t";

    // within each entry, tabs are used, e.g.
    // 0    headerX    0
    // 1    headerX    0
    public string LookupIntraEntryColumnSeparator { get; init; } = "\t";

    //public string ToJson()
    //{

    //    using var stream = File.Create(@"C:\temp\xxxset.json");
    //    using var writer = new StreamWriter(stream);
        
    //    //JsonSerializer.Serialize(stream, this, new JsonSerializerOptions() {IgnoreReadOnlyProperties = false, IgnoreReadOnlyFields = false});
    //    JsonSerializer.Serialize(stream, this, new JsonSerializerOptions() { WriteIndented = true});

    //    return "";
    //}
    
}