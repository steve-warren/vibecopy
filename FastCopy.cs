using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading.Channels;

namespace vibecopy;

public static class FastCopy
{
    private const int SECTOR_SIZE = 4096; // Standard NVMe physical sector size
    private const FileOptions FILE_FLAG_NO_BUFFERING = (FileOptions)0x20000000; // FILE_FLAG_NO_BUFFERING

    public static void CopyPath(
            string sourceDir,
            string destDir,
            ChannelWriter<FileCopyResult> progressWriter,
            int workerCount,
            int bufferSize)
    {
        using var pool = new AlignedMemoryPool(
            workerCount,
            bufferSize,
            SECTOR_SIZE);

        try
        {
            if (!Directory.Exists(sourceDir)) throw new DirectoryNotFoundException();
            Directory.CreateDirectory(destDir);

            var files = new DirectoryInfo(sourceDir)
                .EnumerateFiles("*", SearchOption.AllDirectories);

            Parallel.ForEach(files,
                 new ParallelOptions { MaxDegreeOfParallelism = workerCount },
                 (file, _) =>
                 {
                     var result = ProcessPath(
                         destDir,
                         sourceDir,
                         file,
                         pool);

                     progressWriter.TryWrite(result);
                 });

            CloneDirectoryMetadata(sourceDir, destDir);
        }

        finally
        {
            progressWriter.TryComplete();
        }
    }

    private static FileCopyResult ProcessPath(
        string destDir,
        string sourceDir,
        FileInfo file,
        AlignedMemoryPool pool)
    {
        string destFile = Path.Combine(destDir, Path.GetRelativePath(sourceDir, file.FullName));
        Directory.CreateDirectory(Path.GetDirectoryName(destFile)!);

        var originalCreationTime = file.CreationTimeUtc;
        var originalWriteTime = file.LastWriteTimeUtc;
        var originalAccessTime = file.LastAccessTimeUtc;
        var originalAttributes = file.Attributes;

        var start_ts = Stopwatch.GetTimestamp();

        CopyAndVerifyFile(file.FullName, destFile, pool);

        File.SetCreationTimeUtc(destFile, originalCreationTime);
        File.SetLastWriteTimeUtc(destFile, originalWriteTime);
        File.SetLastAccessTimeUtc(destFile, originalAccessTime);

        // Set attributes absolute last. If the source was ReadOnly,
        // the destination will now be ReadOnly, but our timestamps are already safely set.
        File.SetAttributes(destFile, originalAttributes);

        var elapsed_ts = Stopwatch.GetElapsedTime(start_ts);
        return new FileCopyResult(file.FullName, destFile, file.Length, elapsed_ts);
    }

    private static unsafe void CopyAndVerifyFile(
            string source,
            string dest,
            AlignedMemoryPool pool)
    {
        byte[] expectedHash;

        var bufferPointer = pool.Rent();
        void* buffer = (void*)bufferPointer;

        using (var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            using var src = File.OpenHandle(
                source,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                FileOptions.SequentialScan);

            long length = RandomAccess.GetLength(src);

            using var dst = File.OpenHandle(
                    dest,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    FileOptions.WriteThrough | FILE_FLAG_NO_BUFFERING,
                    length);

            long offset = 0;

            while (offset < length)
            {
                long remaining = length - offset;

                // Read exact logical bytes, but ensure the write request is sector-aligned
                int validBytes = (int)Math.Min(pool.ByteCount, remaining);
                int alignedWriteSize = (int)Math.Min(pool.ByteCount, (remaining + SECTOR_SIZE - 1) & ~(SECTOR_SIZE - 1));

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

        byte[] actualHash;

        using (var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        using (var verify = File.OpenHandle(dest, FileMode.Open, FileAccess.Read, FileShare.Read, FILE_FLAG_NO_BUFFERING))
        {
            long length = RandomAccess.GetLength(verify);
            long offset = 0;

            while (offset < length)
            {
                long remaining = length - offset;

                // Hardware rules: Unbuffered read requests MUST be a multiple of the sector size.
                // We round up the request to the nearest 4096 bytes, but only hash the valid data.
                int bytesToRequest = (int)Math.Min(pool.ByteCount, (remaining + SECTOR_SIZE - 1) & ~(SECTOR_SIZE - 1));
                int validBytes = (int)Math.Min(pool.ByteCount, remaining);

                Span<byte> requestSpan = new(buffer, bytesToRequest);
                int read = RandomAccess.Read(verify, requestSpan, offset);
                if (read == 0) break;

                hasher.AppendData(requestSpan[..validBytes]);
                offset += validBytes; // Only advance offset by actual logical file length
            }
            actualHash = hasher.GetHashAndReset();
        }

        if (!expectedHash.AsSpan().SequenceEqual(actualHash))
        {
            File.Delete(dest);
            throw new IOException($"SILENT CORRUPTION DETECTED ON DISK: {dest}");
        }
    }

    private static void CloneDirectoryMetadata(
        string sourceDir,
        string destDir)
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
            var dstInfo = new DirectoryInfo(destPath)
            {
                Attributes = srcInfo.Attributes
            };
        }
    }
}
