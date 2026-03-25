using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Channels;

namespace vibecopy;

public static class FastCopy
{
    private const int BufferSize = 1 << 20; // 1 MB (Must be a multiple of SectorSize)
    private const int SectorSize = 4096;    // Standard NVMe physical sector size
    private const FileOptions NoBuffering = (FileOptions)0x20000000; // FILE_FLAG_NO_BUFFERING

    public static async Task CopyDirectoryAsync(
            string sourceDir,
            string destDir,
            ChannelWriter<string> progressWriter)
    {
        if (!Directory.Exists(sourceDir)) throw new DirectoryNotFoundException();
        Directory.CreateDirectory(destDir);

        var files = Directory.GetFiles(sourceDir, "*", SearchOption.AllDirectories);

        Parallel.ForEach(files,
            new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1) },
            (file, _) =>
            {
                string destFile = Path.Combine(destDir, Path.GetRelativePath(sourceDir, file));
                Directory.CreateDirectory(Path.GetDirectoryName(destFile)!);

                var srcInfo = new FileInfo(file);
                var originalCreationTime = srcInfo.CreationTimeUtc;
                var originalWriteTime = srcInfo.LastWriteTimeUtc;
                var originalAccessTime = srcInfo.LastAccessTimeUtc;
                var originalAttributes = srcInfo.Attributes;

                CopyAndVerifyStrict(file, destFile);

                File.SetCreationTimeUtc(destFile, originalCreationTime);
                File.SetLastWriteTimeUtc(destFile, originalWriteTime);
                File.SetLastAccessTimeUtc(destFile, originalAccessTime);

                // Set attributes absolute last. If the source was ReadOnly, 
                // the destination will now be ReadOnly, but our timestamps are already safely set.
                File.SetAttributes(destFile, originalAttributes);

                progressWriter.TryWrite(destFile);
            });

        progressWriter.Complete();

        CloneDirectoryMetadata(sourceDir, destDir);
    }

    private static unsafe void CopyAndVerifyStrict(
            string source,
            string dest)
    {
        // Aligned memory is MANDATORY for unbuffered I/O
        void* buffer = NativeMemory.AlignedAlloc(BufferSize, SectorSize);

        try
        {
            byte[] expectedHash;

            // --- PHASE 1: COPY & HASH (Strict Unbuffered DMA) ---
            using (var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                using var src = File.OpenHandle(source, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan);

                long length = RandomAccess.GetLength(src);
                // ADD NoBuffering HERE:
                using var dst = File.OpenHandle(dest, FileMode.CreateNew, FileAccess.Write, FileShare.None, FileOptions.WriteThrough | NoBuffering, length);
                long offset = 0;

                while (offset < length)
                {
                    long remaining = length - offset;

                    // Read exact logical bytes, but ensure the write request is sector-aligned
                    int validBytes = (int)Math.Min(BufferSize, remaining);
                    int alignedWriteSize = (int)Math.Min(BufferSize, (remaining + SectorSize - 1) & ~(SectorSize - 1));

                    Span<byte> readSpan = new(buffer, validBytes);
                    Span<byte> writeSpan = new(buffer, alignedWriteSize);

                    int read = RandomAccess.Read(src, readSpan, offset);
                    if (read == 0) break;

                    // Hash ONLY the valid logical bytes
                    hasher.AppendData(readSpan[..read]);

                    // Write the full aligned block (including any trailing buffer padding)
                    RandomAccess.Write(dst, writeSpan, offset);

                    offset += read; // Advance by logical bytes read
                }

                // TRUNCATE: Fix the file size in the Master File Table (MFT) back to the logical length
                RandomAccess.SetLength(dst, length);

                expectedHash = hasher.GetHashAndReset();
            }

            // --- PHASE 2: PHYSICAL VERIFY (Unbuffered I/O) ---
            byte[] actualHash;

            using (var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            using (var verify = File.OpenHandle(dest, FileMode.Open, FileAccess.Read, FileShare.Read, NoBuffering))
            {
                long length = RandomAccess.GetLength(verify);
                long offset = 0;

                while (offset < length)
                {
                    long remaining = length - offset;

                    // Hardware rules: Unbuffered read requests MUST be a multiple of the sector size.
                    // We round up the request to the nearest 4096 bytes, but only hash the valid data.
                    int bytesToRequest = (int)Math.Min(BufferSize, (remaining + SectorSize - 1) & ~(SectorSize - 1));
                    int validBytes = (int)Math.Min(BufferSize, remaining);

                    Span<byte> requestSpan = new Span<byte>(buffer, bytesToRequest);
                    int read = RandomAccess.Read(verify, requestSpan, offset);
                    if (read == 0) break;

                    hasher.AppendData(requestSpan[..validBytes]);
                    offset += validBytes; // Only advance offset by actual logical file length
                }
                actualHash = hasher.GetHashAndReset();
            }

            // --- PHASE 3: VALIDATE ---
            if (!expectedHash.AsSpan().SequenceEqual(actualHash))
            {
                File.Delete(dest);
                throw new IOException($"SILENT CORRUPTION DETECTED ON DISK: {dest}");
            }
        }
        finally
        {
            NativeMemory.AlignedFree(buffer);
        }
    }

    private static void CloneDirectoryMetadata(string sourceDir, string destDir)
    {
        // Grab all subdirectories, plus the root source directory itself
        var directories = new List<string> { sourceDir };
        directories.AddRange(Directory.GetDirectories(sourceDir, "*", SearchOption.AllDirectories));

        foreach (var srcPath in directories)
        {
            string relPath = Path.GetRelativePath(sourceDir, srcPath);
            // If relPath is ".", we are looking at the root directory
            string destPath = relPath == "." ? destDir : Path.Combine(destDir, relPath);

            if (!Directory.Exists(destPath)) continue;

            var srcInfo = new DirectoryInfo(srcPath);

            // 1. Set Timestamps (UTC to avoid DST drift)
            Directory.SetCreationTimeUtc(destPath, srcInfo.CreationTimeUtc);
            Directory.SetLastWriteTimeUtc(destPath, srcInfo.LastWriteTimeUtc);
            Directory.SetLastAccessTimeUtc(destPath, srcInfo.LastAccessTimeUtc);

            // 2. Set Attributes Last
            // We use DirectoryInfo here because File.SetAttributes can sometimes 
            // behave weirdly with reparse points/symlinks on directories.
            var dstInfo = new DirectoryInfo(destPath);
            dstInfo.Attributes = srcInfo.Attributes;
        }
    }
}
