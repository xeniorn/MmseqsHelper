namespace MmseqsHelperLib;

internal abstract class MmseqsCommandLineParameters : CommandLineParameters
{
    public abstract string CommandString { get; }
}