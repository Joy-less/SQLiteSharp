namespace SQLiteSharp;

/// <summary>
/// A parsed connection string for a <see cref="SQLiteConnection"/>.
/// </summary>
public class SQLiteConnectionString {
    public string UniqueKey { get; }
    public string DatabasePath { get; }
    public object? Key { get; }
    public OpenFlags OpenFlags { get; }
    public Action<SQLiteConnection>? PreKeyAction { get; }
    public Action<SQLiteConnection>? PostKeyAction { get; }

    /// <summary>
    /// Constructs a new SQLiteConnectionString with all the data needed to open an SQLiteConnection.
    /// </summary>
    /// <param name="databasePath">
    /// Specifies the path to the database file.
    /// </param>
    /// <param name="key">
    /// Specifies the encryption key to use on the database. Should be a string or a byte[].
    /// </param>
    /// <param name="preKeyAction">
    /// Executes prior to setting key for SQLCipher databases
    /// </param>
    /// <param name="postKeyAction">
    /// Executes after setting key for SQLCipher databases
    /// </param>
    /// <param name="vfsName">
    /// Specifies the Virtual File System to use on the database.
    /// </param>
    public SQLiteConnectionString(string databasePath, object? key = null, Action<SQLiteConnection>? preKeyAction = null, Action<SQLiteConnection>? postKeyAction = null)
        : this(databasePath, OpenFlags.Create | OpenFlags.ReadWrite, key, preKeyAction, postKeyAction) {
    }

    /// <summary>
    /// Constructs a new SQLiteConnectionString with all the data needed to open an SQLiteConnection.
    /// </summary>
    /// <param name="databasePath">
    /// Specifies the path to the database file.
    /// </param>
    /// <param name="openFlags">
    /// Flags controlling how the connection should be opened.
    /// </param>
    /// <param name="key">
    /// Specifies the encryption key to use on the database. Should be a string or a byte[].
    /// </param>
    /// <param name="preKeyAction">
    /// Executes prior to setting key for SQLCipher databases
    /// </param>
    /// <param name="postKeyAction">
    /// Executes after setting key for SQLCipher databases
    /// </param>
    /// <param name="vfsName">
    /// Specifies the Virtual File System to use on the database.
    public SQLiteConnectionString(string databasePath, OpenFlags openFlags = SQLiteSharp.OpenFlags.Create | SQLiteSharp.OpenFlags.ReadWrite, object? key = null, Action<SQLiteConnection>? preKeyAction = null, Action<SQLiteConnection>? postKeyAction = null) {
        if (key is not null && key is not (string or byte[])) {
            throw new ArgumentException("Encryption key must be string or byte array", nameof(key));
        }

        UniqueKey = $"{databasePath}_{(uint)openFlags:X8}";
        Key = key;
        PreKeyAction = preKeyAction;
        PostKeyAction = postKeyAction;
        OpenFlags = openFlags;

        DatabasePath = databasePath;
    }
}