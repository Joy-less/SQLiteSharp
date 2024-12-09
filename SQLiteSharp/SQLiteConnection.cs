namespace SQLiteSharp;

/// <summary>
/// An open connection to a SQLite database.
/// </summary>
public partial class SqliteConnection : IDisposable {
    /// <summary>
    /// The Object-Relational Mapper to use with the connection.
    /// </summary>
    public Orm Orm { get; }
    /// <summary>
    /// The native SQLite database handle from <see cref="SQLitePCL"/>.
    /// </summary>
    public Sqlite3DatabaseHandle Handle { get; }

    /// <summary>
    /// The options used to open this connection.
    /// </summary>
    public SqliteConnectionOptions Options { get; }

    /// <summary>
    /// Initializes the raw SQLite Portable Class Library.
    /// </summary>
    static SqliteConnection() {
        SQLitePCL.Batteries_V2.Init();
    }

    /// <summary>
    /// Creates a new connection to the given SQLite database.
    /// </summary>
    public SqliteConnection(SqliteConnectionOptions options) {
        Options = options;
        Orm = options.Orm ?? Orm.Default;

        // Try to open database
        Result openResult = SqliteRaw.Open(options.DatabasePath, out Sqlite3DatabaseHandle handle, options.OpenFlags, null);
        if (openResult is not Result.OK) {
            throw new SqliteException(openResult, $"Could not open database file {Options.DatabasePath.SqlQuote()}: {openResult}");
        }
        Handle = handle;

        // Use encryption key
        if (options.EncryptionKey is not null) {
            SqliteRaw.SetKey(Handle, options.EncryptionKey);
        }

        // Create custom collations
        foreach (KeyValuePair<string, Func<string, string, int>> collation in options.Collations) {
            CreateCollation(collation.Key, collation.Value);
        }
    }
    /// <inheritdoc cref="SqliteConnection(SqliteConnectionOptions)"/>
    public SqliteConnection(string databasePath, OpenFlags openFlags = OpenFlags.Recommended)
        : this(new SqliteConnectionOptions(databasePath, openFlags)) {
    }

    /// <summary>
    /// The SQLite library version number. <c>3007014</c> refers to <c>v3.7.14</c>.
    /// </summary>
    public static int SqliteVersionNumber => SqliteRaw.GetLibraryVersionNumber();

    /// <summary>
    /// Closes the connection to the database.
    /// </summary>
    public void Dispose() {
        GC.SuppressFinalize(this);

        if (Handle.IsInvalid) {
            return;
        }

        try {
            SqliteRaw.Close(Handle);
        }
        finally {
            Handle.Dispose();
        }
    }

    /// <summary>
    /// Changes the 256-bit (32-byte) encryption key used to encrypt/decrypt the database.
    /// </summary>
    public void ChangeKey(byte[] key, string dbName = "main") {
        SqliteRaw.ChangeKey(Handle, key, dbName);
    }
    /// <inheritdoc cref="ChangeKey(byte[], string)"/>
    public Task ChangeKeyAsync(byte[] key) {
        return Task.Run(() => ChangeKey(key));
    }

    /// <summary>
    /// Enables or disables <see href="https://sqlite.org/loadext.html">extension loading</see>.<br/>
    /// Default: <see langword="false"/>
    /// </summary>
    public void SetExtensionLoadingEnabled(bool enabled) {
        Result result = SqliteRaw.SetExtensionLoadingEnabled(Handle, enabled ? 1 : 0);
        if (result is not Result.OK) {
            throw new SqliteException(result, SqliteRaw.GetErrorMessage(Handle));
        }
    }
    /// <inheritdoc cref="SetExtensionLoadingEnabled(bool)"/>
    public Task SetExtensionLoadingEnabledAsync(bool enabled) {
        return Task.Run(() => SetExtensionLoadingEnabled(enabled));
    }

    /// <summary>
    /// Sets the <see href="https://www.sqlite.org/c3ref/busy_timeout.html">busy timeout</see>.<br/>
    /// Default: 30 seconds
    /// </summary>
    public void SetBusyTimeout(TimeSpan value) {
        SqliteRaw.SetBusyTimeout(Handle, (int)value.TotalMilliseconds);
    }

    /// <summary>
    /// Creates a collation for string comparison.
    /// </summary>
    /// <remarks>
    /// Redefining an existing collation will break existing indexes using that collation.
    /// </remarks>
    public void CreateCollation(string name, Func<string, string, int> compare) {
        Result result = SqliteRaw.CreateCollation(Handle, name, compare);
        if (result is not Result.OK) {
            throw new SqliteException(result, SqliteRaw.GetErrorMessage(Handle));
        }
    }

    /// <summary>
    /// Gets or creates a table for the given type.<br/>
    /// Indexes are also created for columns with <see cref="IndexAttribute"/>.<br/>
    /// You can create a virtual table using <paramref name="virtualModule"/>.
    /// For example, passing "fts5" creates a virtual table using <see href="https://www.sql-easy.com/learn/sqlite-full-text-search">Full Text Search v5</see>.
    /// </summary>
    public SqliteTable<T> GetTable<T>(string? tableName = null, string? virtualModule = null) where T : notnull, new() {
        return new SqliteTable<T>(this, tableName, virtualModule);
    }
    /// <inheritdoc cref="GetTable{T}(string?, string?)"/>
    public Task<SqliteTable<T>> GetTableAsync<T>(string? tableName = null, string? virtualModule = null) where T : notnull, new() {
        return Task.Run(() => GetTable<T>(tableName, virtualModule));
    }

    /// <summary>
    /// Gets a table for the given type without actually creating it in the database.<br/>
    /// This is useful for retrieving rows from <c>pragma</c> tables such as <c>table_info</c>.
    /// </summary>
    internal SqliteTable<T> GetTablePlaceholder<T>(string? tableName = null) where T : notnull, new() {
        return new SqliteTable<T>(this, tableName, createTable: false);
    }

    /// <summary>
    /// Gets information about each table in the database, as returned from <c>pragma table_list</c>.
    /// </summary>
    /// <param name="tableName">
    /// If not <see langword="null"/>, only returns a result for the table with the given name.
    /// </param>
    public IEnumerable<TableInfo> GetTables(string? tableName = null) {
        string query = $"pragma table_list";
        if (tableName is not null) {
            query += $"({tableName.SqlQuote()})";
        }
        return CreateCommand(query).ExecuteQuery(GetTablePlaceholder<TableInfo>("table_list"));
    }
    /// <inheritdoc cref="GetTables(string?)"/>
    public Task<IEnumerable<TableInfo>> GetTablesAsync() {
        return Task.Run(() => GetTables());
    }

    /// <summary>
    /// Gets information about each column in a table, as returned from <c>pragma table_info</c>.
    /// </summary>
    public IEnumerable<ColumnInfo> GetColumns(string tableName) {
        string query = $"pragma table_info({tableName.SqlQuote()})";
        return CreateCommand(query).ExecuteQuery(GetTablePlaceholder<ColumnInfo>("table_info"));
    }
    /// <inheritdoc cref="GetColumns(string)"/>
    public Task<IEnumerable<ColumnInfo>> GetColumnsAsync(string tableName) {
        return Task.Run(() => GetColumns(tableName));
    }

    /// <summary>
    /// Creates a new SqliteCommand given the command text with parameters.<br/>
    /// Put <c>?</c> in the command text for each argument.
    /// </summary>
    public SqliteCommand CreateCommand(string commandText, params IEnumerable<object?> parameters) {
        SqliteCommand command = new(this) {
            CommandText = commandText,
            Parameters = parameters.Select(parameter => new SqliteCommandParameter(null, parameter)),
        };
        return command;
    }
    /// <summary>
    /// Creates a new SqliteCommand given the command text with named parameters.<br/>
    /// Put <c>@</c> (or <c>:</c> / <c>$</c>) in the command text followed by an identifier for each argument.<br/>
    /// For example, <c>@name</c>, <c>:name</c> or <c>$name</c>.
    /// </summary>
    public SqliteCommand CreateCommand(string commandText, IDictionary<string, object?> parameters) {
        SqliteCommand command = new(this) {
            CommandText = commandText,
            Parameters = parameters.Select(parameter => new SqliteCommandParameter(parameter.Key, parameter.Value)),
        };
        return command;
    }

    /// <summary>
    /// Creates a <see cref="SqliteCommand"/> and executes a non query.<br/>
    /// Use this method when you don't expect rows back.
    /// </summary>
    /// <returns>
    /// The number of rows modified.
    /// </returns>
    public int Execute(string query, params IEnumerable<object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        return command.Execute();
    }
    /// <inheritdoc cref="Execute(string, IEnumerable{object?})"/>
    public Task<int> ExecuteAsync(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => Execute(query, parameters));
    }

    /// <inheritdoc cref="Execute(string, IEnumerable{object?})"/>
    public int Execute(string query, IDictionary<string, object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        return command.Execute();
    }
    /// <inheritdoc cref="Execute(string, IDictionary{string, object?})"/>
    public Task<int> ExecuteAsync(string query, IDictionary<string, object?> parameters) {
        return Task.Run(() => Execute(query, parameters));
    }

    /// <summary>
    /// Creates a <see cref="SqliteCommand"/> and executes a scalar query.<br/>
    /// Use this method retrieve primitive values.
    /// </summary>
    /// <returns>
    /// The first column of each row returned by the query.
    /// </returns>
    public IEnumerable<T> ExecuteScalar<T>(string query, params IEnumerable<object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        return command.ExecuteScalars<T>();
    }
    /// <inheritdoc cref="ExecuteScalar{T}(string, IEnumerable{object?})"/>
    public Task<IEnumerable<T>> ExecuteScalarAsync<T>(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => ExecuteScalar<T>(query, parameters));
    }

    /// <inheritdoc cref="ExecuteScalar{T}(string, IEnumerable{object?})"/>
    public IEnumerable<T> ExecuteScalars<T>(string query, IDictionary<string, object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        return command.ExecuteScalars<T>();
    }
    /// <inheritdoc cref="ExecuteScalars{T}(string, IDictionary{string, object?})"/>
    public Task<IEnumerable<T>> ExecuteScalarsAsync<T>(string query, IDictionary<string, object?> parameters) {
        return Task.Run(() => ExecuteScalars<T>(query, parameters));
    }

    /// <summary>
    /// Creates a transaction or savepoint for commands to be rolled back or committed.<br/>
    /// Call <see cref="Rollback(string?)"/> to cancel the transaction or <see cref="Commit(string?)"/> to perform the transaction.
    /// </summary>
    public void CreateSavePoint(string? savePointName = null) {
        try {
            // Create savepoint
            if (savePointName is not null) {
                Execute($"savepoint {savePointName.SqlQuote()}");
            }
            // Create transaction
            else {
                Execute("begin transaction");
            }
        }
        // Failed to create transaction/savepoint
        catch (Exception) {
            Rollback();
            throw;
        }
    }
    /// <inheritdoc cref="CreateSavePoint(string?)"/>
    public Task SavePointAsync(string? savePointName = null) {
        return Task.Run(() => CreateSavePoint(savePointName));
    }

    /// <summary>
    /// Reverses the transaction to a point created by <see cref="CreateSavePoint(string?)"/>.
    /// </summary>
    public void Rollback(string? savePointName = null) {
        try {
            // Rollback to savepoint
            if (savePointName is not null) {
                Execute($"rollback to {savePointName.SqlQuote()}");
            }
            // Rollback to beginning of transaction
            else {
                Execute("rollback");
            }
        }
        // Failed to rollback transaction/savepoint
        catch (Exception) {
            Rollback();
            throw;
        }
    }
    /// <inheritdoc cref="Rollback(string?)"/>
    public Task RollbackAsync(string? savePointName = null) {
        return Task.Run(() => Rollback(savePointName));
    }

    /// <summary>
    /// Commits the transaction or savepoint created by <see cref="CreateSavePoint(string?)"/>.
    /// </summary>
    public void Commit(string? savePointName = null) {
        try {
            // Commit savepoint
            if (savePointName is not null) {
                Execute($"release {savePointName.SqlQuote()}");
            }
            // Commit transaction
            else {
                Execute("commit");
            }
        }
        // Failed to commit transaction/savepoint
        catch (Exception) {
            Rollback();
            throw;
        }
    }
    /// <inheritdoc cref="Commit(string?)"/>
    public Task CommitAsync(string? savePointName = null) {
        return Task.Run(() => Commit(savePointName));
    }

    /// <summary>
    /// Creates a savepoint with a random <see cref="Guid"/> name, executes the action and commits the transaction.<br/>
    /// The savepoint is rolled back if an exception is thrown.
    /// </summary>
    public void RunInTransaction(Action action) {
        string savePointName = Guid.NewGuid().ToString();
        try {
            CreateSavePoint(savePointName);
            action();
            Commit(savePointName);
        }
        catch (Exception) {
            Rollback(savePointName);
            throw;
        }
    }
    /// <inheritdoc cref="RunInTransaction(Action)"/>
    public Task RunInTransactionAsync(Action action) {
        return Task.Run(() => RunInTransaction(action));
    }

    /// <summary>
    /// Saves a backup of the entire database to the specified path.
    /// </summary>
    public void Backup(string destinationPath, string databaseName = "main") {
        // Create a database at the destination
        Result result = SqliteRaw.Open(destinationPath, out Sqlite3DatabaseHandle destHandle, OpenFlags.Recommended, null);
        if (result is not Result.OK) {
            throw new SqliteException(result, "Failed to open destination database");
        }

        // Initialize the backup
        Sqlite3BackupHandle backupHandle = SqliteRaw.BackupInit(destHandle, databaseName, Handle, databaseName);
        if (backupHandle is null) {
            SqliteRaw.Close(destHandle);
            throw new Exception("Failed to create backup");
        }

        // Run the backup
        SqliteRaw.BackupStep(backupHandle, -1);
        SqliteRaw.BackupFinish(backupHandle);

        // Check for errors
        result = SqliteRaw.GetResult(destHandle);
        string errorMessage = "";
        if (result is not Result.OK) {
            errorMessage = SqliteRaw.GetErrorMessage(destHandle);
        }

        // Close the backup database
        SqliteRaw.Close(destHandle);
        // Report errors
        if (result is not Result.OK) {
            throw new SqliteException(result, errorMessage);
        }
    }
    /// <inheritdoc cref="Backup(string, string)"/>
    public Task BackupAsync(string destinationDatabasePath, string databaseName = "main") {
        return Task.Run(() => Backup(destinationDatabasePath, databaseName));
    }
}