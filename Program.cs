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

    Console.WriteLine($"Copying '{src}' to '{dst}'...");

    var progressChannel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
    {
        SingleReader = true, // Optimizes the channel for a single consumer
        SingleWriter = false
    });

    var task = FastCopy.CopyDirectoryAsync(src, dst, progressChannel.Writer);

    await foreach (var file in progressChannel.Reader.ReadAllAsync())
    {
        Console.WriteLine($"{DateTime.Now} {file}");
    }

    await task;

    Console.WriteLine("✅ Copy & verification completed successfully.");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"❌ Error: {ex.Message}");
    Environment.Exit(1);
}
