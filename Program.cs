using System.Diagnostics;
using System.Threading.Channels;
using vibecopy;

if (args.Length < 2)
{
    Console.Error.WriteLine("Usage: vibecopy <sourceDir> <destDir> [workerCount] [bufferSizeMB]");
    Environment.Exit(1);
}

try
{
    string src = Path.GetFullPath(args[0]);
    string dst = Path.GetFullPath(args[1]);
    string workerCountArg = args.Length > 2 ? args[2] : Environment.ProcessorCount.ToString();
    string bufferSizeArg = args.Length > 3 ? args[3] : "4";

    var workerCount = int.Parse(workerCountArg);
    var bufferSize = Capacity.Int32.Mebibytes(Math.Min(int.Parse(bufferSizeArg), 4));

    Console.WriteLine($"Copying '{src}' to '{dst}'...");

    var progressChannel = Channel.CreateUnbounded<FileCopyResult>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false
    });

    var watch = Stopwatch.StartNew();
    var task = Task.Run(() => FastCopy.CopyPath(
                src,
                dst,
                progressChannel.Writer,
                workerCount,
                bufferSize));

    var totalBytes = 0L;
    var totalFiles = 0L;

    await foreach (var file in progressChannel.Reader.ReadAllAsync())
    {
        Console.WriteLine($"{DateTime.Now} {file.Destination} {Capacity.ToHuman(file.SizeInBytes)} in {file.Duration}.");
        totalBytes += file.SizeInBytes;
        totalFiles++;
    }

    await task;

    Console.WriteLine($"Perfect vibe! Vibed {totalFiles:N0} files, {Capacity.ToHuman(totalBytes)} successfully in {watch.Elapsed}");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Vibe ruined! {ex.Message}");
    Environment.Exit(1);
}
