using FastaHelperLib;

namespace MmseqsHelperLib;

public class ColabfoldPredictionTarget : IProteinPredictionTarget
{
    public ColabfoldPredictionTarget()
    {
    }

    public ColabfoldPredictionTarget(string userProvidedId, List<int> multiplicities, List<Protein> uniqueProteins)
    {
        UserProvidedId = userProvidedId;
        Multiplicities = multiplicities;
        UniqueProteins = uniqueProteins;
    }

    public ColabfoldPredictionTarget(string userProvidedId) : this()
    {
        UserProvidedId = userProvidedId;
    }


    public List<Protein> UniqueProteins { get; set; } = [];
    public List<int> Multiplicities { get; set; } = [];
    public string UserProvidedId { get; set; } = string.Empty;


    public IProteinPredictionTarget Clone()
    {
        var a = new ColabfoldPredictionTarget(userProvidedId: UserProvidedId, multiplicities: new List<int>(Multiplicities),
            uniqueProteins: new List<Protein>(UniqueProteins));

        return a;
    }

    public void AppendProtein(Protein prot, int multiplicity)
    {
        var index = UniqueProteins.FindIndex(x => x.Sequence.Equals(prot.Sequence));
        var found = (index >= 0);

        if (found)
        {
            Multiplicities[index] += multiplicity;
        }
        else
        {
            UniqueProteins.Add(prot);
            Multiplicities.Add(multiplicity);
        }

    }

    public bool SameUniqueProteinsAs(IProteinPredictionTarget other)
    {
        return ((IProteinPredictionTarget)this).SameUniqueProteinsAs(other);
    }
}