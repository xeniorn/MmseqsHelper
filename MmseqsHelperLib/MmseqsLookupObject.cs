using System.Text;

namespace MmseqsHelperLib;

public class MmseqsLookupObject
{
    public MmseqsLookupObject()
    {
        Entries = new Dictionary<int, MmseqsLookupEntry>();
    }

    public MmseqsDatabaseType DatabaseType { get; }

    private Dictionary<int, MmseqsLookupEntry> Entries { get; }
        
    public async Task WriteToFileSystemAsync(AutoMmseqsSettings settings, string dbPath)
    {
        var separator = settings.Mmseqs2Internal_LookupColumnSeparator;
            
        var lookupPath = $"{dbPath}{settings.Mmseqs2Internal_DbLookupSuffix}";
        var lines = Entries.Select(x =>
            string.Join(separator, x.Value.EntryIndex, x.Value.ReferenceName, x.Value.PairingGroup));

        var data = Encoding.ASCII.GetBytes(string.Join("\n", lines));

        await File.WriteAllBytesAsync(lookupPath, data);
            
    }
        
    public void Add(int targetIndex, string proteinId, int pairGroup)
    {
        if (Entries.ContainsKey(targetIndex))
        {
            throw new ArgumentException("Entry with this index already exists");
        }
        Entries.Add(targetIndex, new MmseqsLookupEntry(targetIndex, proteinId, pairGroup));
            
    }
}