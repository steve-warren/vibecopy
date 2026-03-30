using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace vibecopy;

public sealed unsafe class AlignedMemoryPool : IDisposable
{
    private readonly ConcurrentBag<nint> _pool = [];

    public AlignedMemoryPool(
        int capacity,
        int byteCount,
        int alignment)
    {
        Capacity = capacity;
        ByteCount = byteCount;
        Alignment = alignment;

        try
        {
            for (var i = 0; i < capacity; i++)
            {
                var ptr = Alloc(byteCount, alignment);
                _pool.Add(ptr);
            }
        }

        catch (ArgumentException)
        {
            // occurs when alignment is not ^2
            // this would fail on the fist alloc
            throw;
        }

        catch (OutOfMemoryException)
        {
            foreach (var ptr in _pool)
                Free(ptr);

            _pool.Clear();

            throw;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static nint Alloc(int byteCount, int alignment) =>
        (nint)NativeMemory.AlignedAlloc((nuint)byteCount, (nuint)alignment);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Free(nint ptr) =>
        NativeMemory.AlignedFree((void*)ptr);

    public int ByteCount { get; }
    public int Alignment { get; }
    public int Capacity { get; }

    public void Dispose()
    {
        foreach (var ptr in _pool)
            Free(ptr);

        _pool.Clear();
    }

    public nint Rent()
    {
        return _pool.TryTake(out var buffer)
        ? buffer
        : throw new InvalidOperationException("pool exhausted.");
    }

    public void Return(nint buffer)
    {
        _pool.Add(buffer);
    }
}
