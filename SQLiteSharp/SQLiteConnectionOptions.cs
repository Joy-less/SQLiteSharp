namespace SQLiteSharp;

/// <summary>
/// Settings for opening a <see cref="SqliteConnection"/>.
/// </summary>
public record struct SqliteConnectionOptions(string DatabasePath, OpenFlags OpenFlags = OpenFlags.Recommended) {
    public string DatabasePath { get; set; } = DatabasePath;
    public OpenFlags OpenFlags { get; set; } = OpenFlags;
    public byte[]? EncryptionKey { get; set; } = null;
    public Orm? Orm { get; set; } = null;
    public Dictionary<string, Func<string, string, int>> Collations { get; set; } = new() {
        [Collation.Invariant] = (string str1, string str2) => string.Compare(str1, str2, StringComparison.InvariantCulture),
        [Collation.Invariant_NoCase] = (string str1, string str2) => string.Compare(str1, str2, StringComparison.InvariantCultureIgnoreCase),
    };
}