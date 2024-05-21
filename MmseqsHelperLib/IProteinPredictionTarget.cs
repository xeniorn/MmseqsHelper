using FastaHelperLib;

namespace MmseqsHelperLib;

public interface IProteinPredictionTarget
{
    public bool IsMono => UniqueProteins.Count == 1 && Multiplicities.Single() == 1;
    public bool IsHomo => UniqueProteins.Count == 1 && Multiplicities.Single() > 1;
    public bool IsHetero => UniqueProteins.Count > 1;
    public bool IsComplex => IsHetero || IsHomo;
    public bool IsHomoComplex => IsComplex && IsHomo;
    public bool IsHeteroComplex => IsHetero;

    public List<Protein> UniqueProteins { get;}
    public List<int> Multiplicities { get; }
    public string UserProvidedId { get; set; }

    public string AutoId => string.IsNullOrWhiteSpace(UserProvidedId) ? AutoIdFromConstituents : $"{UserProvidedId}#{AutoIdFromConstituents}";

    public string AutoIdFromConstituents
    {
        get
        {
            var a = UniqueProteins.Zip(Multiplicities).OrderBy(x => x.First).ThenByDescending(x => x.Second)
                .Select(x => x.First.Id + (x.Second == 1 ? "" : $"[{x.Second}x]"));
            return string.Join("_vs_", a);
        }
    }

    public int TotalLength
    {
        get
        {
            var lengths = UniqueProteins.Select(x => x.Sequence.Length);
            var multiplicities = Multiplicities;

            var total = lengths.Zip(multiplicities, (len, multi) => len * multi).Sum();
            return total;
        }
    }

    public bool SameUniqueProteinsAs(IProteinPredictionTarget other)
    {
        if (ReferenceEquals(this, other)) return true;
        if (this.UniqueProteins.Count != other.UniqueProteins.Count) return false;

        foreach (var uniqueProtein in this.UniqueProteins)
        {
            if (!other.UniqueProteins.Any(x => x.SameSequenceAs(uniqueProtein))) return false;
        }

        return true;

    }
}