using DotNetBrightener.LinQToSqlBuilder;

namespace SQLiteSharp;

/// <summary>
/// An open connection to a SQLite database.
/// </summary>
public partial class SqliteConnection : IDisposable {
    public Orm Orm { get; } = new();
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

        Result openResult = SqliteRaw.Open(options.DatabasePath, out Sqlite3DatabaseHandle handle, options.OpenFlags, null);
        Handle = handle;

        if (openResult is not Result.OK) {
            throw new SqliteException(openResult, $"Could not open database file {Options.DatabasePath.SqlQuote()}: {openResult}");
        }

        BusyTimeout = TimeSpan.FromSeconds(1.0);

        if (options.EncryptionKey is not null) {
            SqliteRaw.SetKey(Handle, options.EncryptionKey);
        }
    }
    /// <inheritdoc cref="SqliteConnection(SqliteConnectionOptions)"/>
    public SqliteConnection(string databasePath, OpenFlags openFlags = OpenFlags.Recommended)
        : this(new SqliteConnectionOptions(databasePath, openFlags)) {
    }

    /// <summary>
    /// The SQLite library version number. <c>3007014</c> refers to <c>v3.7.14</c>.
    /// </summary>
    public static int SqliteVersionNumber => SqliteRaw.LibVersionNumber();

    /// <summary>
    /// When an operation can't be completed because a table is locked, the operation will be regularly repeated until <see cref="BusyTimeout"/> has elapsed.
    /// </summary>
    public TimeSpan BusyTimeout {
        get => field;
        set {
            field = value;
            SqliteRaw.BusyTimeout(Handle, (int)field.TotalMilliseconds);
        }
    }

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
    /// Enables or disables <see href="https://sqlite.org/loadext.html">extension loading</see>.
    /// </summary>
    public void SetExtensionLoadingEnabled(bool enabled) {
        Result result = SqliteRaw.SetExtensionLoadingEnabled(Handle, enabled ? 1 : 0);
        if (result is not Result.OK) {
            string errorMessage = SqliteRaw.GetErrorMessage(Handle);
            throw new SqliteException(result, errorMessage);
        }
    }
    /// <inheritdoc cref="SetExtensionLoadingEnabled(bool)"/>
    public Task EnableLoadExtensionAsync(bool enabled) {
        return Task.Run(() => SetExtensionLoadingEnabled(enabled));
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
    /// Gets information about each column in a table.
    /// </summary>
    public IEnumerable<ColumnInfo> GetTableInfo(string tableName) {
        string query = $"pragma table_info({tableName.SqlQuote()})";
        return CreateCommand(query).ExecuteQuery(GetTablePlaceholder<ColumnInfo>("table_info"));
    }
    /// <inheritdoc cref="GetTableInfo(string)"/>
    public Task<IEnumerable<ColumnInfo>> GetTableInfoAsync(string tableName) {
        return Task.Run(() => GetTableInfo(tableName));
    }

    /// <summary>
    /// Returns true if the <c>table_info</c> pragma returns any rows.
    /// </summary>
    /// <remarks>
    /// Tables must have at least one row in SQLite.
    /// </remarks>
    public bool TableExists(string tableName) {
        return GetTableInfo(tableName).Any();
    }
    /// <inheritdoc cref="TableExists(string)"/>
    public Task<bool> TableExistsAsync(string tableName) {
        return Task.Run(() => TableExists(tableName));
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
    /// Creates a <see cref="SqliteCommand"/> and executes a multiple scalar query.<br/>
    /// Use this method retrieve multiple primitive values.
    /// </summary>
    /// <returns>
    /// The first column of each row returned by the query.
    /// </returns>
    public IEnumerable<T> ExecuteQueryScalars<T>(string query, params IEnumerable<object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        return command.ExecuteQueryScalars<T>();
    }
    /// <inheritdoc cref="ExecuteQueryScalars{T}(string, IEnumerable{object?})"/>
    public Task<IEnumerable<T>> ExecuteQueryScalarsAsync<T>(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => ExecuteQueryScalars<T>(query, parameters));
    }

    /// <inheritdoc cref="ExecuteQueryScalars{T}(string, IEnumerable{object?})"/>
    public IEnumerable<T> ExecuteQueryScalars<T>(string query, IDictionary<string, object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        return command.ExecuteQueryScalars<T>();
    }
    /// <inheritdoc cref="ExecuteQueryScalars{T}(string, IDictionary{string, object?})"/>
    public Task<IEnumerable<T>> ExecuteQueryScalarsAsync<T>(string query, IDictionary<string, object?> parameters) {
        return Task.Run(() => ExecuteQueryScalars<T>(query, parameters));
    }

    /// <summary>
    /// Creates a transaction or savepoint for commands to be rolled back or committed.<br/>
    /// Call <see cref="Rollback(string?)"/> to cancel the transaction or <see cref="Commit(string?)"/> to perform the transaction.
    /// </summary>
    public void CreateTransaction(string? savePointName = null) {
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
    /// <inheritdoc cref="CreateTransaction(string?)"/>
    public Task CreateTransactionAsync(string? savePointName = null) {
        return Task.Run(() => CreateTransaction(savePointName));
    }

    /// <summary>
    /// Rolls back the transaction to a point begun by <see cref="BeginTransaction()"/> or <see cref="CreateTransaction(string)"/>.
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
    /// Commits the transaction that was begun by <see cref="BeginTransaction()"/> or <see cref="CreateTransaction(string)"/>.
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
            CreateTransaction(savePointName);
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