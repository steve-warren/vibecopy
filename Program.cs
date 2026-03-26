using System.Diagnostics;
using System.Threading.Channels;
using vibecopy;

if (args.Length != 2)
{
    Console.Error.WriteLine("Usage: vibecopy <sourceDir> <destDir>");
    Environment.Exit(1);
}

try
{
    string src = Path.GetFullPath(args[0]);
    string dst = Path.GetFullPath(args[1]);
    string workerCountArg = args.Length > 2 ? args[2] : Math.Min(3, Environment.ProcessorCount).ToString();

    var workerCount = int.Parse(workerCountArg);

    Console.WriteLine($"Copying '{src}' to '{dst}'...");

    var progressChannel = Channel.CreateUnbounded<FileCopyResult>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false
    });

    var watch = Stopwatch.StartNew();
    var task = Task.Run(() => FastCopy.CopyDirectory(src, dst, progressChannel.Writer, workerCount));

    var totalBytes = 0UL;
    var totalFiles = 0UL;

    await foreach (var file in progressChannel.Reader.ReadAllAsync())
    {
        Console.WriteLine($"{DateTime.Now} {file.Destination} {file.SizeInBytes:N0} bytes.");
        totalBytes += file.SizeInBytes;
        totalFiles++;
    }

    await task;

    Console.WriteLine($"Copied and verified {totalFiles} files, {totalBytes:N0} bytes successfully in {watch.Elapsed}");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Error: {ex.Message}");
    Environment.Exit(1);
}
