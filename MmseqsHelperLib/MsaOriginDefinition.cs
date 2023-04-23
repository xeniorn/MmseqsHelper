using FastaHelperLib;

namespace MmseqsHelperLib;

public record MsaOriginDefinition(int StartLine, int EndLine, MmseqsSourceDatabaseTarget SourceDatabaseTarget, List<Protein> Proteins, bool IsThisPaired, bool IsEmpty);