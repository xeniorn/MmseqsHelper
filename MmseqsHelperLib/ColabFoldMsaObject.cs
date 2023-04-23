using System.Net;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AlphafoldPredictionLib;
using FastaHelperLib;
using Microsoft.Extensions.Logging;

namespace MmseqsHelperLib;

public class ColabFoldMsaObject
{
    public ColabFoldMsaObject(List<AnnotatedMsaData> dataEntries, PredictionTarget predictionTarget)
    {
        DataEntries = dataEntries;
        PredictionTarget = predictionTarget;
    }

    public List<AnnotatedMsaData> DataEntries { get; set; } = new ();
    public PredictionTarget PredictionTarget { get; }
    private ColabfoldMsaMetadataInfo Metadata { get; set; }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="settings"></param>
    /// <param name="targetFolder"></param>
    /// <param name="instanceInfo"></param>
    /// <returns></returns>
    /// this is bad - it includes the ColabfoldHelper-specific object... but this is designed to be an implementation-agnostic representation of CFMsaObject.
    /// TODO: refactor this. Possibly just put it out of this? Or use extension method?
    public async Task WriteToFileSystemAsync(ColabfoldMmseqsHelperSettings settings, string targetFolder, ColabfoldHelperComputationInstanceInfo instanceInfo)
    {
        Metadata = new ColabfoldMsaMetadataInfo(predictionTarget: PredictionTarget, createTime: DateTime.Now,
            computationInstanceInfo: instanceInfo, settings: settings);

        var fullMsaPath = Path.Join(targetFolder, settings.PersistedA3mDbConfig.ResultA3mFilename);
        var fullInfoPath = Path.Join(targetFolder, settings.PersistedA3mDbConfig.A3mInfoFilename);
        var writeTasks = new List<Task>()
        {
            File.WriteAllBytesAsync(fullMsaPath, GetBytes()),
            Metadata.WriteToFileSystemAsync(fullInfoPath)
        };
        await Task.WhenAll(writeTasks);
    }

    private byte[] GetBytes()
    {
        var text = GetString();
        var bytes = Encoding.ASCII.GetBytes(text);
        return bytes;
    }
    private IEnumerable<string> GetPairedLines(Dictionary<Protein, byte[]> pairedData)
    {
        if (!pairedData.Any()) yield break;

        var proteinToPairedLineList = new List<(Protein protein, string[] a3mLines)>();
        foreach (var (protein, bytes) in pairedData)
        {
            var lines = Encoding.ASCII.GetString(bytes).Split("\n");
            proteinToPairedLineList.Add((protein, lines));
        }

        var lineCount = proteinToPairedLineList.First().a3mLines.Length;
        if (proteinToPairedLineList.Any(x => x.a3mLines.Length != lineCount))
        {
            throw new ArgumentException("Paired reads have unequal number of lines, this should not happen.");
        }
        // each entry has two lines; header and data
        var entryCount = lineCount / 2;

        for (var entry = 0; entry < entryCount; entry++)
        {
            const char FastaHeaderSymbol = '>';

            var headerIndex = 2 * entry;
            var dataIndex = headerIndex + 1;

            var headerLine = FastaHeaderSymbol + string.Join("\t", proteinToPairedLineList.Select(x => x.a3mLines[headerIndex].Trim().TrimStart(FastaHeaderSymbol)));
            var dataLine = string.Join("", proteinToPairedLineList.Select(x => x.a3mLines[dataIndex]));

            yield return headerLine;
            yield return dataLine;
        }
    }

    private string GetString()
    {
        var lines = new List<string>();
        var commentLine = $"#{String.Join(",", PredictionTarget.UniqueProteins.Select(x => x.Sequence.Length))}\t{String.Join(",", PredictionTarget.Multiplicities)}";
        lines.Add(commentLine);

        foreach (var entry in DataEntries.Where(x=>x.DataType == ColabfoldMsaDataType.Paired))
        {
            var nextAvailableLine = lines.Count();
            var pairLines = GetPairedLines(entry.DataDict).ToList();
            
            var startLine = nextAvailableLine;
            var endLine = nextAvailableLine + pairLines.Count - 1;
            
            var isEmpty = !pairLines.Any();
            if (!isEmpty)
            {
                Metadata.MsaOriginDefinitions.Add(new MsaOriginDefinition(startLine, endLine, entry.SourceDatabaseTarget, PredictionTarget.UniqueProteins, true, false));
            }
            else
            {
                Metadata.MsaOriginDefinitions.Add(new MsaOriginDefinition(-1, -1, entry.SourceDatabaseTarget, PredictionTarget.UniqueProteins, true, true));
            }
            
            lines.AddRange(pairLines);
        }

        foreach (var protein in PredictionTarget.UniqueProteins)
        {
            foreach (var entry in DataEntries.Where(x => x.DataType == ColabfoldMsaDataType.Unpaired))
            {
                var nextAvailableLine = lines.Count();
                var unpairedLines = GetUnpairedLines(entry.DataDict, protein).ToList();

                var startLine = nextAvailableLine;
                var endLine = nextAvailableLine + unpairedLines.Count - 1;

                var isEmpty = !unpairedLines.Any();
                if (!isEmpty)
                {
                    Metadata.MsaOriginDefinitions.Add(new MsaOriginDefinition(startLine, endLine, entry.SourceDatabaseTarget, new List<Protein>(){ protein } , false, false));
                }
                else
                {
                    Metadata.MsaOriginDefinitions.Add(new MsaOriginDefinition(-1, -1, entry.SourceDatabaseTarget, new List<Protein>() { protein }, false, true));
                }

                lines.AddRange(unpairedLines);
            }
        }

        // don't forget the terminal newline
        return $"{string.Join("\n", lines)}\n";

    }

    private IEnumerable<string> GetUnpairedLines(Dictionary<Protein, byte[]> unpairedData, Protein protein)
    {
        const char fastaGapSymbol = '-';

        var unpairedLineList = Encoding.ASCII.GetString(unpairedData[protein]).Split("\n");

        var index = PredictionTarget.UniqueProteins.IndexOf(protein);
        var lengthProteinsBefore = PredictionTarget.UniqueProteins.Where((_, i) => i < index).Select(x => x.Sequence.Length).Sum();
        var lengthProteinsAfter = PredictionTarget.UniqueProteins.Where((_, i) => i > index).Select(x => x.Sequence.Length).Sum();

        var dataPreAppendGap = new string(fastaGapSymbol, lengthProteinsBefore);
        var dataPostAppendGap = new string(fastaGapSymbol, lengthProteinsAfter);

        var lineCount = unpairedLineList.Length;
        var entryCount = lineCount / 2;

        for (var entry = 0; entry < entryCount; entry++)
        {
            var headerIndex = 2 * entry;
            var dataIndex = headerIndex + 1;

            var headerLine = unpairedLineList[headerIndex].Trim();
            var dataLine = string.Join("", dataPreAppendGap, unpairedLineList[dataIndex], dataPostAppendGap);

            yield return headerLine;
            yield return dataLine;
        }
    }
}

public class ColabfoldHelperComputationInstanceInfo
{
    public string HelperDatabaseVersion { get; set; }
    public string? MmseqsVersion { get; set; }
    public string ComputerIdentifier { get; set; }
    
    public void InitalizeFromSettings(ColabfoldMmseqsHelperSettings settings)
    {
        ComputerIdentifier = GetComputerId(settings.ComputingConfig.TrackingConfig);
    }

    private string GetComputerId(TrackingStrategyConfiguration trackingStrategy)
    {
        string identifier;

        try
        {
            switch (trackingStrategy.ComputerIdentifierSource)
            {
                case TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.None:
                    identifier = string.Empty; break;
                case TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.FirstNetworkInterface:
                    var ni = NetworkInterface.GetAllNetworkInterfaces().First();
                    var niId = ni.Id;
                    identifier = niId.ToString();
                    break;
                case TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.HostName:
                    var ipProperties = IPGlobalProperties.GetIPGlobalProperties();
                    var fqdn = string.Format("{0}{1}", ipProperties.HostName, string.IsNullOrWhiteSpace(ipProperties.DomainName) ? "" : $".{ipProperties.DomainName}" );
                    identifier = fqdn;
                    ;
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
        catch (Exception ex)
        {
            //_logger.LogInformation("Failed to get computer identification based on strategy, will use empty identifier.");
            trackingStrategy.ComputerIdentifierSource = TrackingStrategyConfiguration.ComputerIdentifierSourceStrategy.None;
            identifier = string.Empty;
        }

        return identifier;
    }
}