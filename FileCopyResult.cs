namespace vibecopy;

public readonly record struct FileCopyResult(string Source, string Destination, ulong SizeInBytes);

