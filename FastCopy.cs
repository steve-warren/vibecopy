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

        await Parallel.ForEachAsync(files,
            new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
            async (file, ct) =>
            {
                string destFile = Path.Combine(destDir, Path.GetRelativePath(sourceDir, file));
                Directory.CreateDirectory(Path.GetDirectoryName(destFile)!);

                // Offload the synchronous unsafe work to a thread pool worker
                await Task.Run(() => CopyAndVerifyStrict(file, destFile, ct), ct);

                File.SetLastWriteTime(destFile, File.GetLastWriteTime(file));

                progressWriter.TryWrite(destFile);
            });

        progressWriter.Complete();
    }

    private static unsafe void CopyAndVerifyStrict(
            string source,
            string dest,
            CancellationToken cancellationToken)
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
                    cancellationToken.ThrowIfCancellationRequested();
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
                    cancellationToken.ThrowIfCancellationRequested();
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
}
