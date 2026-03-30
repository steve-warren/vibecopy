namespace vibecopy;

public readonly record struct FileCopyResult(
    string Source,
    string Destination,
    long SizeInBytes,
    TimeSpan Duration);
