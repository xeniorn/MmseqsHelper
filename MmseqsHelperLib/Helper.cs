using System.Diagnostics;
using System.Text;
using AlphafoldPredictionLib;
using FastaHelperLib;

namespace MmseqsHelperLib;

public static partial class Helper
{
    public static async Task CopyFileIfExistsAsync(string sourcePath, string targetPath)
    {
        if (File.Exists(sourcePath))
        {
            await Task.Run(() => File.Copy(sourcePath, targetPath));
        }
    }

    /// <summary>
    /// (!!!!!!!!!!!!!!) Redirecting io streams doesn't seem to work yet
    /// </summary>
    /// <param name="fileName"></param>
    /// <param name="args"></param>
    /// <param name="envVarsToSet"></param>
    /// <param name="liveIoStreams"></param>
    /// <returns></returns>
    public static async Task<int> RunProcessWithLiveStreamsAsync(string fileName, string args,
        (Stream? output, Stream? error) liveIoStreams)
    {
        var useOut = (liveIoStreams.output is not null);
        var useErr = (liveIoStreams.error is not null);

        using var process = new Process()
        {
            StartInfo =
            {
                FileName = fileName, Arguments = args,
                UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardOutput = useOut,
                RedirectStandardError = useErr,
                RedirectStandardInput =  false //useOut || useErr, - someone said this would fix it not working but it didn't
            },
            EnableRaisingEvents = true
        };

        return await RunProcessWithLiveStreamsAsync(process, liveIoStreams.output, liveIoStreams.error); //.ConfigureAwait(false);
    }


    public static async Task<int> RunProcessAsync(string fileName, string args, Dictionary<string, string>? envVarsToSet = null)
    {
        var startInfo = new ProcessStartInfo(fileName, args )
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
            RedirectStandardInput = false
        };

        if (envVarsToSet is not null)
        {
            foreach (var (name, value) in envVarsToSet)
            {
                // this also creates it if it doesn't exist, apparently
                // https://stackoverflow.com/questions/14553830/set-environment-variables-for-a-process/14582921#14582921
                startInfo.EnvironmentVariables[name] = value;
            }
        }
        
        using var process = new Process() { StartInfo = startInfo};

        return await RunProcessAsync(process).ConfigureAwait(false);
    }

    private static Task<int> RunProcessAsync(Process process)
    {
        var tcs = new TaskCompletionSource<int>();

        // important to EnableRaisingEvents, there is no exit event otherwise!
        process.EnableRaisingEvents = true;
        process.Exited += (s, ea) => tcs.SetResult(process.ExitCode);

        bool started = process.Start();
        if (!started)
        {
            tcs.SetResult(int.MinValue);
        }

        return tcs.Task;
    }

    private static Task<int> RunProcessWithLiveStreamsAsync(Process process, Stream? outStream = null, Stream? errStream = null)
    {
        var tcs = new TaskCompletionSource<int>();

        var useOutStream = outStream is not null;
        var useErrStream = errStream is not null;

        process.Exited += (s, ea) => tcs.SetResult(process.ExitCode);

        if (useOutStream)
        {
            process.OutputDataReceived += (s, ea) =>
            {
                if (ea.Data is not null && ea.Data.Length > 0)
                {
                    var message = ea.Data;
                    outStream!.Write(Encoding.ASCII.GetBytes(message));
                }
            };
        }

        if (useErrStream)
        {
            process.ErrorDataReceived += (s, ea) =>
            {
                if (ea.Data is not null && ea.Data.Length > 0)
                {
                    var message = ea.Data;
                    errStream!.Write(Encoding.ASCII.GetBytes(message));
                }
            };
        }

        bool started = process.Start();
        if (!started)
        {
            //you may allow for the process to be re-used (started = false) 
            //but I'm not sure about the guarantees of the Exited event in such a case
            throw new InvalidOperationException("Could not start process: " + process);
        }

        if (useOutStream) process.BeginOutputReadLine();
        if (useErrStream) process.BeginErrorReadLine();

        return tcs.Task;
    }

    public static string GetMd5Hash(string source)
    {
        using System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create();

        var inputBytes = Encoding.ASCII.GetBytes(source);
        var hashBytes = md5.ComputeHash(inputBytes);

        return Convert.ToHexString(hashBytes);
    }

    public static string EnsureQuotedIfWhiteSpace(string input)
    {
        if (!HasWhitespace(input)) return input;
        return IsQuoted(input) ? input : $"\"{input}\"";
    }

    private static bool HasWhitespace(string input) => input.Any(x => Char.IsWhiteSpace(x));
    private static bool IsQuoted(string input) => input.Length > 2 && input.First() == '"' && input.Last() == '"';

    public static string GetAutoHashIdWithoutMultiplicity(PredictionTarget predictionTarget)
    {
        var standardSortedPredictionTarget = Helper.GetStandardSortedPredictionTarget(predictionTarget);
        var orderedSequences = standardSortedPredictionTarget.UniqueProteins.Select(x=>x.Sequence);

        var individualSequenceHashes = orderedSequences.Select(Helper.GetMd5Hash);
        var sequenceBasedHashBasis = string.Join("", individualSequenceHashes);
        var hashId = Helper.GetMd5Hash(sequenceBasedHashBasis);

        return hashId;
    }

    public static PredictionTarget GetStandardSortedPredictionTarget(PredictionTarget predictionTarget)
    {
        var protWithCount = new List<(Protein protein, int multiplicity)>();
        for (var i = 0; i < predictionTarget.UniqueProteins.Count; i++)
        {
            var sourceSeq = predictionTarget.UniqueProteins[i].Sequence.ToUpper();
            var protein = new Protein() { Id = GetMd5Hash(sourceSeq), Sequence = sourceSeq };
            protWithCount.Add((protein, predictionTarget.Multiplicities[i]));
        }

        var sorted = protWithCount
            .OrderByDescending(x=>x.protein.Sequence.Length)
            .ThenBy(x=>x.protein.Sequence).ToList();

        var result = new PredictionTarget(userProvidedId: predictionTarget.UserProvidedId);

        sorted.ForEach(x=> result.AppendProtein(x.protein, x.multiplicity));
        
        return result;

    }

    public static async Task<string[]> GetDirectoriesAsync(string location)
    {
        return await Task.Run(() => Directory.GetDirectories(location));
    }

    public static async Task<string[]> GetFilesAsync(string location)
    {
        return await Task.Run(() => Directory.GetFiles(location));
    }

    public static string GetStandardizedDbName(string dbName)
    {
        return dbName.ToUpper().Replace(" ", "").Replace("-", "_");
    }

    public static async Task CreateDirectoryAsync(string directory)
    {
        await Task.Run(() => Directory.CreateDirectory(directory));
    }

    public static string GetMultimerName(PredictionTarget pt)
    {
        try
        {
            var number = pt.UniqueProteins.Count;
            return GetMultimerName(number);
        }
        catch (Exception ex)
        {
            return string.Empty;
        }

    }

    public static string RemoveSuffix(string input, string suffix)
    {
        if (input.Length < suffix.Length) return input;
        
        var output = input;

        while (output.EndsWith(suffix))
        {
            output = output.Substring(0, output.Length - suffix.Length);
        }

        return output;
    }

    public static async Task CreateFileAsync(string file)
    {
        await Task.Run(() => File.Create(file));
    }

    public static void Touch(string fileName)
    {
        FileStream myFileStream = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Write);
        myFileStream.Close();
        myFileStream.Dispose();
        File.SetLastWriteTimeUtc(fileName, DateTime.UtcNow);
    }

    public static string GetMultimerName(int numberOfMonomers)
    {
        switch (numberOfMonomers)
        {
            case 1: return "monomer";
            case 2: return "dimer";
            case 3: return "trimer";
            case 4: return "tetramer";
            case 5: return "pentamer";
            case 6: return "hexamer";
            case 7: return "heptamer";
            case 8: return "monomer";
            case 9: return "9-mer";
            case 10: return "decamer";
            case 11: return "11-mer";
            case 12: return "dodecamer";
            default: return $"{numberOfMonomers}-mer";
        }
    }

    public static async Task<(int exitCode, string output)> RunProcessAndGetStdoutAsync(string fileName, string args)
    {
        var useOut = true;
        var useErr = true;

        using var process = new Process()
        {
            StartInfo =
            {
                FileName = fileName, 
                Arguments = args,
                UseShellExecute = false, 
                CreateNoWindow = true,
                RedirectStandardOutput = useOut,
                RedirectStandardError = useErr,
                RedirectStandardInput =  false //useOut || useErr, - someone said this would fix it not working but it didn't
            },
        };

        process.Start();
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();

        return (process.ExitCode,  output);
    }
}