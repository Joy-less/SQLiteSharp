namespace SQLiteSharp;

/// <summary>
/// Settings for opening a <see cref="SqliteConnection"/>.
/// </summary>
public record struct SqliteConnectionOptions(string DatabasePath) {
    /// <summary>
    /// The file path to the database or ":memory:" for a temporary in-memory database.
    /// </summary>
    public string DatabasePath { get; set; } = DatabasePath;
    /// <summary>
    /// The options for opening the native database connection. You should not need to change this from the default.
    /// </summary>
    public OpenFlags OpenFlags { get; set; } = OpenFlags.Recommended;
    /// <summary>
    /// The optional 256-bit (32-byte) encryption key to encrypt/decrypt the database.
    /// </summary>
    public byte[]? EncryptionKey { get; set; } = null;
    /// <summary>
    /// The Object-Relational Mapper to map CLR members to SQLite columns.
    /// </summary>
    public Orm Orm { get; set; } = Orm.Default;
    /// <summary>
    /// The string comparison collations to create in addition to the built-in collations.<br/><br/>
    /// By default:
    /// <list type="bullet">
    /// <item><c>INVARIANT</c>: Compares the strings with case-sensitive unicode characters.</item>
    /// <item><c>INVARIANT_NOCASE</c>: Compares the strings with case-insensitive unicode characters.</item>
    /// </list>
    /// </summary>
    public Dictionary<string, Func<string, string, int>> Collations { get; set; } = new() {
        [Collation.Invariant] = (string str1, string str2) => string.Compare(str1, str2, StringComparison.InvariantCulture),
        [Collation.InvariantNoCase] = (string str1, string str2) => string.Compare(str1, str2, StringComparison.InvariantCultureIgnoreCase),
    };
    /// <summary>
    /// Whether to enable foreign key constraints.<br/>
    /// The default is <see langword="true"/>.
    /// </summary>
    public bool EnableForeignKeys { get; set; } = true;
}