namespace MmseqsHelperLib;

public class CommandLineParameters
{
    public CommandLineParameters()
    {
        this.Parameters = new List<ICommandLineParameter>();
    }

    public void ApplyDefaults()
    {
        Parameters.ForEach(x=>x.Value=x.DefaultValue);
    }

    public List<ICommandLineParameter> Parameters { get; protected init; }

    public IEnumerable<ICommandLineParameter> GetNonDefault()
    {
        return Parameters.Where(x=> x.DefaultValue.CompareTo(x.Value) != 0);
    }

}