using System.Text;

namespace MmseqsHelperLib;

public class MmseqsLookupObject
{
    public MmseqsLookupObject()
    {
        Entries = new Dictionary<int, MmseqsLookupEntry>();
    }
    
    private Dictionary<int, MmseqsLookupEntry> Entries { get; }
        
    public async Task WriteToFileSystemAsync(MmseqsSettings settings, string dbPath)
    {
        var separator = settings.Mmseqs2Internal.LookupIntraEntryColumnSeparator;
            
        var lookupPath = $"{dbPath}{settings.Mmseqs2Internal.DbLookupSuffix}";

        // 2023-04-09 DAMN no. needs a terminal newline at each line, not just between entries. Disaster.
        //var lines = Entries.Select(x =>
        //    string.Join(separator, x.Value.EntryIndex, x.Value.ReferenceName, x.Value.PairingGroup));
        //var data = Encoding.ASCII.GetBytes(string.Join("\n", lines));

        var lines = Entries.Select(x =>
            string.Join(separator, 
                x.Value.EntryIndex, 
                x.Value.ReferenceName, 
                x.Value.PairingGroup) 
            + settings.Mmseqs2Internal.LookupEntryTerminator);
        var data = Encoding.ASCII.GetBytes(string.Join("", lines));

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