namespace MmseqsHelperLib;

public class NonParametrizedMmseqsOptions
{
    public HashSet<MmseqsNonParametrizedOption> Options { get; set; } = new();

    public static NonParametrizedMmseqsOptions? Combine(IEnumerable<NonParametrizedMmseqsOptions?> sourceOptions)
    {
        NonParametrizedMmseqsOptions? res = null;
            
        foreach (var options in sourceOptions)
        {
            if (options is null) continue;
            if (res is null) res = new NonParametrizedMmseqsOptions();

            foreach (var option in options.Options)
            {
                res.Options.Add(option);
            }
        }

        return res;
    }
}