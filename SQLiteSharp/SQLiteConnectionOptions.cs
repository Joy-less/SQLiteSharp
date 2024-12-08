namespace SQLiteSharp;

/// <summary>
/// Settings for opening a <see cref="SqliteConnection"/>.
/// </summary>
public record struct SqliteConnectionOptions(string DatabasePath, OpenFlags OpenFlags = OpenFlags.Recommended, byte[]? EncryptionKey = null, Orm? Orm = null) {
    public string DatabasePath { get; set; } = DatabasePath;
    public OpenFlags OpenFlags { get; set; } = OpenFlags;
    public byte[]? EncryptionKey { get; set; } = EncryptionKey;
    public Orm? Orm { get; set; } = Orm;
}