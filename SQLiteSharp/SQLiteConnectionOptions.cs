namespace SQLiteSharp;

/// <summary>
/// Settings for opening a <see cref="SQLiteConnection"/>.
/// </summary>
public record struct SQLiteConnectionOptions(
    string DatabasePath,
    OpenFlags OpenFlags = OpenFlags.Recommended,
    byte[]? EncryptionKey = null,
    ObjectMapper? mapper = null
);