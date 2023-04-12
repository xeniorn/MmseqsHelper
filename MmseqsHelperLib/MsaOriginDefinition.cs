namespace MmseqsHelperLib;

public record MsaOriginDefinition(int StartLine, int EndLine, MmseqsSourceDatabaseTarget SourceDatabaseTarget,
    bool IsThisPaired, bool IsEmpty);