namespace SQLiteSharp;

/// <summary>
/// Settings for opening a <see cref="SqliteConnection"/>.
/// </summary>
public record struct SqliteConnectionOptions(
    string DatabasePath,
    OpenFlags OpenFlags = OpenFlags.Recommended,
    byte[]? EncryptionKey = null,
    Orm? Mapper = null
);