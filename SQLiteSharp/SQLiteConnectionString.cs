namespace SQLiteSharp;

/// <summary>
/// A parsed connection string for a <see cref="SQLiteConnection"/>.
/// </summary>
public class SQLiteConnectionString(string databasePath, OpenFlags openFlags = OpenFlags.Create | OpenFlags.ReadWrite, byte[]? key = null) {
    public string DatabasePath { get; } = databasePath;
    public OpenFlags OpenFlags { get; } = openFlags;
    public byte[]? Key { get; } = key;
}