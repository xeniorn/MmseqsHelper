namespace MmseqsHelperLib;

public enum CommandLineFeature
{
    MustHaveValue,
    CanAcceptValueAfterWhiteSpace,
    CanAcceptValueAfterEqualsSign,
    Mandatory,
    AffectsOnlyVerbosityOrSimilar,
    CanAcceptValueWithoutSpacer
}