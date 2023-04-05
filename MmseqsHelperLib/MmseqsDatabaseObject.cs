﻿using System.Text;

namespace MmseqsHelperLib;

public class MmseqsDatabaseObject
{
    public MmseqsDatabaseObject(MmseqsDatabaseType type)
    {
        this.DatabaseType = type;
        Entries = new Dictionary<int, byte[]>();
    }

    public MmseqsDatabaseType DatabaseType { get; }

    private Dictionary<int, byte[]> Entries { get; }

    public int Add(byte[] data)
    {
        var nextIndex = Entries.Count > 0 ? Entries.Keys.Max() + 1 : 0;
        Add(data, nextIndex);
        return nextIndex;
    }

    public void Add(byte[] data, int targetIndex)
    {
        if (Entries.ContainsKey(targetIndex))
        {
            throw new ArgumentException("Entry with this index already exists");
        }
        Entries.Add(targetIndex, data);
    }

    public async Task WriteToFileSystemAsync(AutoMmseqsSettings settings, string dbPath)
    {
        var aggregateOffset = 0;

        var separator = Encoding.ASCII.GetBytes(settings.Mmseqs2Internal_DataEntrySeparator);
        var newline = Encoding.ASCII.GetBytes("\n");

        var dataDbPath = $"{dbPath}{settings.Mmseqs2Internal_DbDataSuffix}";
        var indexDbPath = $"{dbPath}{settings.Mmseqs2Internal_DbIndexSuffix}";
        var dbTypePath = $"{dbPath}{settings.Mmseqs2Internal_DbTypeSuffix}";

        var dataFragments = Entries.Values.ToList();
        var totalDataLength = dataFragments.Select(x => x.Length + separator.Length).Sum();

        await using var dataWriteStream = new FileStream(dataDbPath, FileMode.CreateNew, FileAccess.Write,
            FileShare.None, bufferSize: totalDataLength, useAsync: true);

        await using var indexWriteStream = new FileStream(indexDbPath, FileMode.CreateNew, FileAccess.Write,
            FileShare.None, bufferSize: 4096, useAsync: true);

        // this might be quite inefficient
        foreach (var (index, data) in Entries)
        {
            var entryDataLength = data.Length + settings.Mmseqs2Internal_DataEntrySeparator.Length;
            var indexFragment = new MmseqsIndexEntry(index, aggregateOffset, entryDataLength);
            aggregateOffset += entryDataLength;

            await dataWriteStream.WriteAsync(data);
            await dataWriteStream.WriteAsync(separator);

            await indexWriteStream.WriteAsync(GenerateMmseqsEntryLine(indexFragment, settings));
            await indexWriteStream.WriteAsync(newline);
        }

        await WriteDbTypeFileAsync(dbTypePath);
    }

    private async Task WriteDbTypeFileAsync(string dbTypePath)
    {
        var dbTypeBytes = new byte[4];

        switch (DatabaseType)
        {
            case MmseqsDatabaseType.Sequence_AMINO_ACIDS:
                dbTypeBytes = new byte[] { 0, 0, 0, 0 };
                break;
            case MmseqsDatabaseType.Sequence_NUCLEOTIDES:
                dbTypeBytes = new byte[] { 1, 0, 0, 0 };
                break;
            case MmseqsDatabaseType.A3m_MSA_DB:
                dbTypeBytes = new byte[] { 11, 0, 0, 0 };
                break;
            case MmseqsDatabaseType.Alignment_ALIGNMENT_RES:
                // got this from the generated header of my alignment files, don't know the explanation for the "2" part
                dbTypeBytes = new byte[] { 5, 0, 2, 0 };
                break;
            case MmseqsDatabaseType.Header_GENERIC_DB:
                dbTypeBytes = new byte[] { 12, 0, 0, 0 };
                break;
            default:
                throw new ArgumentException("Unimplemented DatabaseType");
        }

        await File.WriteAllBytesAsync(dbTypePath, dbTypeBytes);

    }

    private byte[] GenerateMmseqsEntryLine(MmseqsIndexEntry mmseqsIndexEntry, AutoMmseqsSettings settings)
    {
        var separator = settings.Mmseqs2Internal_IndexColumnSeparator;
        var resultString = string.Join(separator, mmseqsIndexEntry.Index, mmseqsIndexEntry.StartOffset,
            mmseqsIndexEntry.Length);
        return Encoding.ASCII.GetBytes(resultString);
    }
}