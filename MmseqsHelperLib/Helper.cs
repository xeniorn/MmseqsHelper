using System.Diagnostics;
using System.Text;

namespace MmseqsHelperLib;

public static class Helper
{
    public static async Task CopyFileIfExistsAsync(string sourcePath, string targetPath)
    {
        if (File.Exists(sourcePath))
        {
            await Task.Run(() => File.Copy(sourcePath, targetPath));
        }
    }

    public static async Task<int> RunProcessAsync(string fileName, string args, (Stream? output, Stream? error)? outStreams = null)
    {
        using var process = new Process
        {
            StartInfo =
                {
                    FileName = fileName, Arguments = args,
                    UseShellExecute = false, CreateNoWindow = true,
                    RedirectStandardOutput = (outStreams?.output is not null), RedirectStandardError = (outStreams?.error is not null)
                },
            EnableRaisingEvents = true
        };

        return await RunProcessAsync(process).ConfigureAwait(false);
    }

    private static Task<int> RunProcessAsync(Process process, Stream? outStream = null, Stream? errStream = null)
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


}