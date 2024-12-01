namespace SQLiteSharp;

[Flags]
public enum CreateFlags {
    /// <summary>
    /// Use the default creation options.
    /// </summary>
    None = 1,
    /// <summary>
    /// Create a primary key index for a property called 'Id' (case-insensitive).<br/>
    /// This avoids the need for the [<see cref="PrimaryKeyAttribute"/>].
    /// </summary>
    ImplicitPrimaryKey = 2,
    /// <summary>
    /// Create indices for properties ending in 'Id' (case-insensitive).
    /// </summary>
    ImplicitIndex = 4,
    /// <summary>
    /// Force the primary key property to be auto incrementing.<br/>
    /// This avoids the need for [<see cref="AutoIncrementAttribute"/>].<br/>
    /// The primary key must be an integer.
    /// </summary>
    AutoIncrementPrimaryKey = 8,
    /// <summary>
    /// Create a virtual table using <see href="https://www.sqlite.org/fts3.html">FTS3</see>.
    /// </summary>
    FullTextSearch3 = 16,
    /// <summary>
    /// Create a virtual table using <see href="https://www.sqlite.org/fts3.html">FTS4</see>.
    /// </summary>
    FullTextSearch4 = 32,
    /// <summary>
    /// Create a virtual table using <see href="https://www.sqlite.org/fts5.html">FTS5</see>.
    /// </summary>
    FullTextSearch5 = 64,
}

/// <summary>
/// Flags used when accessing a SQLite database, as defined in <see href="https://www.sqlite.org/c3ref/c_open_autoproxy.html">Flags For File Open Operations</see>.
/// </summary>
[Flags]
public enum OpenFlags {
    ReadOnly = 0x00000001,
    ReadWrite = 0x00000002,
    Create = 0x00000004,
    DeleteOnClose = 0x00000008,
    Exclusive = 0x00000010,
    AutoProxy = 0x00000020,
    Uri = 0x00000040,
    Memory = 0x00000080,
    MainDb = 0x00000100,
    TempDb = 0x00000200,
    TransientDb = 0x00000400,
    MainJournal = 0x00000800,
    TempJournal = 0x00001000,
    SubJournal = 0x00002000,
    SuperJournal = 0x00004000,
    NoMutex = 0x00008000,
    FullMutex = 0x00010000,
    SharedCache = 0x00020000,
    PrivateCache = 0x00040000,
    Wal = 0x00080000,
    NoFollow = 0x01000000,
    ExresCode = 0x02000000,
}

public enum CreateTableResult {
    Created,
    Migrated,
}