using System.Numerics;
using System.Runtime.CompilerServices;

namespace vibecopy;

public static class Capacity
{
    private const long BytesPerKibibyte = 1024;
    private const long BytesPerMebibyte = 1024 * 1024;
    private const long BytesPerGibibyte = 1024 * 1024 * 1024;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Mebibytes(long mebibytes) =>
        checked(mebibytes * BytesPerMebibyte);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Kibibytes(long kibibytes) =>
        checked(kibibytes * BytesPerKibibyte);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Gibibytes(long gibibytes) =>
        checked(gibibytes * BytesPerGibibyte);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToHuman(long bytes)
    {
        if (bytes <= 0) return "0 bytes";

        var index = BitOperations.Log2((ulong)bytes) / 10;
        index = Math.Min(index, 5 - 1); // 5 is the number of supported units

        var unit = index switch
        {
            0 => "Bytes",
            1 => "KiB",
            2 => "MiB",
            3 => "GiB",
            4 => "TiB",
            _ => throw new ArgumentException("capacity too large.")
        };

        long size = bytes >> (index * 10);

        return $"{size:N0} {unit}";
    }

    public static class Int32
    {
        private const int Int32BytesPerKibibyte = 1024;
        private const int Int32BytesPerMebibyte = 1024 * 1024;
        private const int Int32BytesPerGibibyte = 1024 * 1024 * 1024;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Mebibytes(int mebibytes) =>
            checked(mebibytes * Int32BytesPerMebibyte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Kibibytes(int kibibytes) =>
            checked(kibibytes * Int32BytesPerKibibyte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Gibibytes(int gibibytes) =>
            checked(gibibytes * Int32BytesPerGibibyte);
    }

    public static class UIntPtr
    {
        private const nuint UIntPtrBytesPerKibibyte = 1024;
        private const nuint UIntPtrBytesPerMebibyte = 1024 * 1024;
        private const nuint UIntPtrBytesPerGibibyte = 1024 * 1024 * 1024;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint Mebibytes(nuint mebibytes) =>
            checked(mebibytes * UIntPtrBytesPerMebibyte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint Kibibytes(nuint kibibytes) =>
            checked(kibibytes * UIntPtrBytesPerKibibyte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint Gibibytes(nuint gibibytes) =>
            checked(gibibytes * UIntPtrBytesPerGibibyte);
    }
}
