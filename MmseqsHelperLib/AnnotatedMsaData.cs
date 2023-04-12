using FastaHelperLib;

namespace MmseqsHelperLib;

public class AnnotatedMsaData
{
    public AnnotatedMsaData(ColabfoldMsaDataType dataType, MmseqsSourceDatabaseTarget sourceDatabaseTarget, Dictionary<Protein, byte[]> dataDict)
    {
        DataType = dataType;
        SourceDatabaseTarget = sourceDatabaseTarget;
        DataDict = dataDict;
    }

    public Dictionary<Protein, byte[]> DataDict { get; }
    public ColabfoldMsaDataType DataType { get; }
    public MmseqsSourceDatabaseTarget SourceDatabaseTarget { get; }
}