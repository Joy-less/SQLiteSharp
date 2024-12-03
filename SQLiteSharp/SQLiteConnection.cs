using System.Collections;
using System.Reflection;
using System.Linq.Expressions;

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
    /// The SQLite library version number. <c>3007014</c> refers to <c>v3.7.14</c>.
    /// </summary>
    public static int SQLiteVersionNumber => SqliteRaw.LibVersionNumber();

    /// <summary>
    /// Changes the 256-bit (32-byte) encryption key used to encrypt/decrypt the database.
    /// </summary>
    public void ChangeKey(byte[] key) {
        SqliteRaw.ChangeKey(Handle, key);
    }

    /// <summary>
    /// Enable or disable extension loading.
    /// </summary>
    public void EnableLoadExtension(bool enabled) {
        Result result = SqliteRaw.EnableLoadExtension(Handle, enabled ? 1 : 0);
        if (result is not Result.OK) {
            string errorMessage = SqliteRaw.GetErrorMessage(Handle);
            throw new SqliteException(result, errorMessage);
        }
    }

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
    /// Gets or creates a table for the given type.<br/>
    /// Indexes are also created for columns with <see cref="IndexedAttribute"/>.<br/>
    /// You can create a virtual table using <paramref name="virtualModule"/>.
    /// For example, passing "fts5" creates a virtual table using <see href="https://www.sql-easy.com/learn/sqlite-full-text-search">Full Text Search v5</see>.
    /// </summary>
    public SqliteTable<T> GetTable<T>(string? tableName = null, string? virtualModule = null) where T : new() {
        return new SqliteTable<T>(this, tableName, virtualModule);
    }

    /// <summary>
    /// Gets information about each column in a table.
    /// </summary>
    public IEnumerable<ColumnInfo> GetTableInfo(string tableName) {
        string query = $"pragma table_info({tableName.SqlQuote()})";
        return CreateCommand(query).ExecuteQuery<ColumnInfo>(GetTable(tableName));
    }
    public bool TableExists(string tableName) {
        return GetTableInfo(tableName).Any();
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
    public SqliteCommand CreateCommand(string commandText, Dictionary<string, object> parameters) {
        SqliteCommand command = new(this) {
            CommandText = commandText,
            Parameters = parameters.Select(parameter => new SqliteCommandParameter(parameter.Key, parameter.Value)),
        };
        return command;
    }

    /// <summary>
    /// Creates and executes a <see cref="SqliteCommand"/> non-query.<br/>
    /// Use this method when you don't expect rows back.
    /// </summary>
    /// <returns>
    /// The number of rows modified.
    /// </returns>
    public int Execute(string query, params IEnumerable<object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        int rowCount = command.ExecuteNonQuery();
        return rowCount;
    }
    /// <summary>
    /// Creates and executes a <see cref="SqliteCommand"/> scalar-query.<br/>
    /// Use this method retrieve primitive values.
    /// </summary>
    /// <returns>
    /// The number of rows modified in the database as a result of this execution.
    /// </returns>
    public T ExecuteScalar<T>(string query, params IEnumerable<object?> parameters) {
        SqliteCommand command = CreateCommand(query, parameters);
        T rowCount = command.ExecuteScalar<T>();
        return rowCount;
    }

    /// <summary>
    /// Creates a SqliteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// It returns the first column of each row of the result.
    /// </summary>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// An enumerable with one result for the first column of each row returned by the query.
    /// </returns>
    public List<T> QueryScalars<T>(string query, params IEnumerable<object?> parameters) {
        return CreateCommand(query, parameters).ExecuteQueryScalars<T>().ToList();
    }

    /// <summary>
    /// Creates a queryable interface to the table associated with the given type.
    /// </summary>
    /// <returns>
    /// A queryable object that can perform <c>Where</c>, <c>OrderBy</c>, <c>Count</c>, <c>Take</c> and <c>Skip</c> queries on the table.
    /// </returns>
    public TableQuery<T> Table<T>() where T : new() {
        return new TableQuery<T>(this);
    }

    /// <summary>
    /// Creates a transaction or savepoint for commands to be rolled back or committed.<br/>
    /// Call <see cref="Rollback(string?)"/> to cancel the transaction or <see cref="Commit(string?)"/> to perform the transaction.
    /// </summary>
    public void SavePoint(string? savePointName = null) {
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
    /// <summary>
    /// Rolls back the transaction to a point begun by <see cref="BeginTransaction()"/> or <see cref="SavePoint(string)"/>.
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
    /// <summary>
    /// Commits the transaction that was begun by <see cref="BeginTransaction()"/> or <see cref="SavePoint(string)"/>.
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
    /// <summary>
    /// Creates a savepoint with a random name, executes the action and commits the transaction.<br/>
    /// The action is rolled back on failure.
    /// </summary>
    public void RunInTransaction(Action action) {
        string savePointName = Guid.NewGuid().ToString();
        try {
            SavePoint(savePointName);
            action();
            Commit(savePointName);
        }
        catch (Exception) {
            Rollback(savePointName);
            throw;
        }
    }

    /// <summary>
    /// Inserts the given object into the table, updating any auto-incremented primary keys.<br/>
    /// The <paramref name="modifier"/> is literal SQL added after <c>INSERT</c> (e.g. <c>OR REPLACE</c>).
    /// </summary>
    /// <returns>The number of rows added.</returns>
    public int Insert(object obj, string? modifier = null) {
        if (obj is null) {
            return 0;
        }

        SqliteTable map = MapTable(obj.GetType());

        SqliteColumn[] columns = map.Columns;

        // Don't insert auto-incremented columns (unless "OR REPLACE"/"OR IGNORE")
        if (string.IsNullOrEmpty(modifier)) {
            columns = [.. columns.Where(column => !column.IsAutoIncrement)];
        }

        object?[] values = new object[columns.Length];
        for (int i = 0; i < values.Length; i++) {
            values[i] = columns[i].GetValue(obj);
        }

        string query;
        if (columns.Length == 0) {
            query = $"insert {modifier} into {map.TableName.SqlQuote()} default values";
        }
        else {
            string columnsSql = string.Join(",", columns.Select(column => column.Name.SqlQuote()));
            string valuesSql = string.Join(",", columns.Select(column => "?"));
            query = $"insert {modifier} into {map.TableName.SqlQuote()}({columnsSql}) values ({valuesSql})";
        }

        int rowCount = Execute(query, values);

        if (map.HasAutoIncrementedPrimaryKey) {
            long rowId = SqliteRaw.GetLastInsertRowId(Handle);
            map.SetPrimaryKeyValue(obj, rowId);
        }

        return rowCount;
    }
    /// <summary>
    /// Inserts each object into the table, updating any auto-incremented primary keys.<br/>
    /// The <paramref name="modifier"/> is literal SQL added after <c>INSERT</c> (e.g. <c>OR REPLACE</c>).
    /// </summary>
    /// <returns>The number of rows added.</returns>
    public int InsertAll(IEnumerable objects, string? modifier = null) {
        int counter = 0;
        RunInTransaction(() => {
            foreach (object obj in objects) {
                counter += Insert(obj, modifier);
            }
        });
        return counter;
    }
    /// <summary>
    /// Inserts the given object into the table, updating any auto-incremented primary keys.<br/>
    /// The <paramref name="modifier"/> is literal SQL added after <c>INSERT</c> (e.g. <c>OR REPLACE</c>).
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the old object is replaced.
    /// </remarks>
    /// <returns>The number of rows added/modified.</returns>
    public int InsertOrReplace(object obj) {
        return Insert(obj, "OR REPLACE");
    }
    /// <summary>
    /// Inserts each object into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the old object is replaced.
    /// </remarks>
    /// <returns>The number of rows added/modified.</returns>
    public int InsertOrReplaceAll(IEnumerable objects) {
        int counter = 0;
        RunInTransaction(() => {
            foreach (object obj in objects) {
                counter += InsertOrReplace(obj);
            }
        });
        return counter;
    }
    /// <summary>
    /// Inserts the given object into the table, updating any auto-incremented primary keys.<br/>
    /// The <paramref name="modifier"/> is literal SQL added after <c>INSERT</c> (e.g. <c>OR REPLACE</c>).
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the new object is not inserted.
    /// </remarks>
    /// <returns>The number of rows modified.</returns>
    public int InsertOrIgnore(object obj) {
        return Insert(obj, "OR IGNORE");
    }
    /// <summary>
    /// Inserts each object into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the new object is not inserted.
    /// </remarks>
    /// <returns>The number of rows added/modified.</returns>
    public int InsertOrIgnoreAll(IEnumerable objects) {
        int counter = 0;
        RunInTransaction(() => {
            foreach (object obj in objects) {
                counter += InsertOrIgnore(obj);
            }
        });
        return counter;
    }

    /// <summary>
    /// Updates all of the columns of a table using the specified object except for its primary key.<br/>
    /// The table must have a designated primary key.
    /// </summary>
    /// <returns>
    /// The number of rows updated.
    /// </returns>
    public int Update(object obj) {
        if (obj is null) {
            return 0;
        }

        SqliteTable map = MapTable(obj.GetType());

        SqliteColumn primaryKey = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't update in table '{map.TableName}' since it has no primary key");

        IEnumerable<SqliteColumn> columns = map.Columns.Where(column => column != primaryKey);
        IEnumerable<object?> values = columns.Select(column => column.GetValue(obj));
        List<object?> parameters = new(values);
        if (parameters.Count == 0) {
            // There is a primary key but no accompanying data,
            // so reset the primary key to make the UPDATE work.
            columns = map.Columns;
            values = columns.Select(column => column.GetValue(obj));
            parameters = new List<object?>(values);
        }
        parameters.Add(primaryKey.GetValue(obj));
        string query = $"update {map.TableName.SqlQuote()} set {string.Join(",", columns.Select(column => $"{column.Name.SqlQuote()} = ? "))} where \"{primaryKey.Name}\" = ?";

        int rowCount = Execute(query, parameters);
        return rowCount;
    }
    /// <inheritdoc cref="Update(object)"/>
    public int UpdateAll(IEnumerable objects) {
        int counter = 0;
        RunInTransaction(() => {
            foreach (object obj in objects) {
                counter += Update(obj);
            }
        });
        return counter;
    }

    /// <summary>
    /// Deletes the object with the specified primary key.
    /// </summary>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public int Delete(object primaryKey, SqliteTable map) {
        SqliteColumn primaryKeyColumn = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't delete in table '{map.TableName}' since it has no primary key");
        string query = $"delete from {map.TableName.SqlQuote()} where {primaryKeyColumn.Name.SqlQuote()} = ?";
        int rowCount = Execute(query, primaryKey);
        return rowCount;
    }
    /// <inheritdoc cref="Delete(object, SqliteTable)"/>
    public int Delete<T>(object primaryKey) {
        return Delete(primaryKey, MapTable<T>());
    }
    /// <summary>
    /// Deletes the given object from the database using its primary key.
    /// </summary>
    /// <param name="objectToDelete">
    /// The object to delete. It must have a primary key designated with <see cref="PrimaryKeyAttribute"/>.
    /// </param>
    /// <returns>
    /// The number of rows deleted.
    /// </returns>
    public int Delete(object objectToDelete) {
        SqliteTable map = MapTable(objectToDelete.GetType());
        return Delete(map.PrimaryKey?.GetValue(objectToDelete)!, map);
    }
    /// <inheritdoc cref="Delete(object)"/>
    public int DeleteAll(IEnumerable objects) {
        int counter = 0;
        RunInTransaction(() => {
            foreach (object obj in objects) {
                counter += Delete(obj);
            }
        });
        return counter;
    }

    /// <summary>
    /// Deletes every object from the specified table.<br/>
    /// Be careful using this.
    /// </summary>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public int DeleteAll(SqliteTable map) {
        string query = $"delete from {map.TableName.SqlQuote()}";
        int rowCount = Execute(query);
        return rowCount;
    }
    /// <inheritdoc cref="DeleteAll(SqliteTable)"/>
    public int DeleteAll<T>() {
        return DeleteAll(MapTable<T>());
    }

    /// <summary>
    /// Saves a backup of the entire database to the specified path.
    /// </summary>
    public void Backup(string destinationDatabasePath, string databaseName = "main") {
        // Open the destination
        Result result = SqliteRaw.Open(destinationDatabasePath, out Sqlite3DatabaseHandle destHandle, OpenFlags.Recommended, null);
        if (result is not Result.OK) {
            throw new SqliteException(result, "Failed to open destination database");
        }

        // Init the backup
        Sqlite3BackupHandle backupHandle = SqliteRaw.BackupInit(destHandle, databaseName, Handle, databaseName);
        if (backupHandle is null) {
            SqliteRaw.Close(destHandle);
            throw new Exception("Failed to create backup");
        }

        // Perform it
        SqliteRaw.BackupStep(backupHandle, -1);
        SqliteRaw.BackupFinish(backupHandle);

        // Check for errors
        result = SqliteRaw.GetResult(destHandle);
        string errorMessage = "";
        if (result is not Result.OK) {
            errorMessage = SqliteRaw.GetErrorMessage(destHandle);
        }

        // Close everything and report errors
        SqliteRaw.Close(destHandle);
        if (result is not Result.OK) {
            throw new SqliteException(result, errorMessage);
        }
    }

    /// <inheritdoc cref="EnableLoadExtension(bool)"/>
    public Task EnableLoadExtensionAsync(bool enabled) {
        return Task.Run(() => EnableLoadExtension(enabled));
    }
    /// <inheritdoc cref="MapTable(Type)"/>
    public Task<SqliteTable> GetMappingAsync(Type type) {
        return Task.Run(() => MapTable(type));
    }
    /// <inheritdoc cref="GetMappingAsync()"/>
    public Task<SqliteTable> GetMappingAsync<T>() where T : new() {
        return Task.Run(() => MapTable<T>());
    }
    /// <inheritdoc cref="GetTable(Type, string?)"/>
    public Task<bool> CreateTableAsync(Type type, string? virtualModuleName = null) {
        return Task.Run(() => GetTable(type, virtualModuleName));
    }
    /// <inheritdoc cref="CreateTable{T}(string?)"/>
    public Task<bool> CreateTableAsync<T>(string? virtualModuleName = null) where T : new() {
        return Task.Run(() => CreateTable<T>(virtualModuleName));
    }
    /// <inheritdoc cref="CreateTables(IEnumerable{Type})"/>
    public Task<Dictionary<Type, bool>> CreateTablesAsync(IEnumerable<Type> types) {
        return Task.Run(() => CreateTables(types));
    }
    /// <inheritdoc cref="DropTable{T}()"/>
    public Task<int> DropTableAsync<T>() where T : new() {
        return Task.Run(() => DropTable<T>());
    }
    /// <inheritdoc cref="DropTable(SqliteTable)"/>
    public Task<int> DropTableAsync(SqliteTable map) {
        return Task.Run(() => DropTable(map));
    }
    /// <inheritdoc cref="GetTableInfo(string)"/>
    public Task<IEnumerable<ColumnInfo>> GetTableInfoAsync(string tableName) {
        return Task.Run(() => GetTableInfo(tableName));
    }
    /// <inheritdoc cref="CreateIndex(string, string, IEnumerable{string}, bool)"/>
    public Task CreateIndexAsync(string indexName, string tableName, IEnumerable<string> columnNames, bool unique = false) {
        return Task.Run(() => CreateIndex(indexName, tableName, columnNames, unique));
    }
    /// <inheritdoc cref="CreateIndex(string, IEnumerable{string}, bool)"/>
    public Task CreateIndexAsync(string tableName, IEnumerable<string> columnNames, bool unique = false) {
        return Task.Run(() => CreateIndex(tableName, columnNames, unique));
    }
    /// <inheritdoc cref="CreateIndex{T}(Expression{Func{T, object}}, bool)"/>
    public Task CreateIndexAsync<T>(Expression<Func<T, object>> property, bool unique = false) {
        return Task.Run(() => CreateIndex(property, unique));
    }
    /// <inheritdoc cref="Insert(object, string?)"/>
    public Task<int> InsertAsync(object obj, string? modifier = null) {
        return Task.Run(() => Insert(obj, modifier));
    }
    /// <inheritdoc cref="InsertAll(IEnumerable, string?)"/>
    public Task<int> InsertAllAsync(IEnumerable objects, string? modifier = null) {
        return Task.Run(() => InsertAll(objects, modifier));
    }
    /// <inheritdoc cref="InsertOrReplace(object)"/>
    public Task<int> InsertOrReplaceAsync(object obj) {
        return Task.Run(() => InsertOrReplace(obj));
    }
    /// <inheritdoc cref="InsertOrReplaceAll(IEnumerable)"/>
    public Task<int> InsertOrReplaceAllAsync(IEnumerable objects) {
        return Task.Run(() => InsertOrReplaceAll(objects));
    }
    /// <inheritdoc cref="InsertOrIgnore(object)"/>
    public Task<int> InsertOrIgnoreAsync(object obj) {
        return Task.Run(() => InsertOrIgnore(obj));
    }
    /// <inheritdoc cref="InsertOrIgnoreAll(IEnumerable)"/>
    public Task<int> InsertOrIgnoreAllAsync(IEnumerable objects) {
        return Task.Run(() => InsertOrIgnoreAll(objects));
    }
    /// <inheritdoc cref="Update(object)"/>
    public Task<int> UpdateAsync(object obj) {
        return Task.Run(() => Update(obj));
    }
    /// <inheritdoc cref="UpdateAll(IEnumerable)"/>
    public Task<int> UpdateAllAsync(IEnumerable objects) {
        return Task.Run(() => UpdateAll(objects));
    }
    /// <inheritdoc cref="Delete(object, SqliteTable)"/>
    public Task<int> DeleteAsync(object primaryKey, SqliteTable map) {
        return Task.Run(() => Delete(primaryKey, map));
    }
    /// <inheritdoc cref="Delete{T}(object)"/>
    public Task<int> DeleteAsync<T>(object primaryKey) {
        return Task.Run(() => Delete<T>(primaryKey));
    }
    /// <inheritdoc cref="Delete(object)"/>
    public Task<int> DeleteAsync(object objectToDelete) {
        return Task.Run(() => Delete(objectToDelete));
    }
    /// <inheritdoc cref="DeleteAll(SqliteTable)"/>
    public Task<int> DeleteAllAsync(SqliteTable map) {
        return Task.Run(() => DeleteAll(map));
    }
    /// <inheritdoc cref="DeleteAll{T}()"/>
    public Task<int> DeleteAllAsync<T>() {
        return Task.Run(() => DeleteAll<T>());
    }
    /// <inheritdoc cref="Backup(string, string)"/>
    public Task BackupAsync(string destinationDatabasePath, string databaseName = "main") {
        return Task.Run(() => Backup(destinationDatabasePath, databaseName));
    }
    /// <inheritdoc cref="Get(object, SqliteTable)"/>
    public Task<object> GetAsync(object pk, SqliteTable map) {
        return Task.Run(() => Get(pk, map));
    }
    /// <inheritdoc cref="Get{T}(object)"/>
    public Task<T> GetAsync<T>(object primaryKey) where T : new() {
        return Task.Run(() => Get<T>(primaryKey));
    }
    /// <inheritdoc cref="Get{T}(Expression{Func{T, bool}})"/>
    public Task<T> GetAsync<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return Task.Run(() => Get<T>(predicate));
    }
    /// <inheritdoc cref="Find(object, SqliteTable)"/>
    public Task<object?> FindAsync(object pk, SqliteTable map) {
        return Task.Run(() => Find(pk, map));
    }
    /// <inheritdoc cref="Find(object)"/>
    public Task<T?> FindAsync<T>(object pk) where T : new() {
        return Task.Run(() => Find<T>(pk));
    }
    /// <inheritdoc cref="Find{T}(Expression{Func{T, bool}})"/>
    public Task<T?> FindAsync<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return Task.Run(() => Find(predicate));
    }
    /// <inheritdoc cref="FindWithQuery{T}(string, IEnumerable{object?})"/>
    public Task<T?> FindWithQueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return Task.Run(() => FindWithQuery<T>(query, parameters));
    }
    /// <inheritdoc cref="FindWithQuery(SqliteTable, string, IEnumerable{object?})"/>
    public Task<object?> FindWithQueryAsync(SqliteTable map, string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => FindWithQuery(map, query, parameters));
    }
    /// <inheritdoc cref="Execute(string, IEnumerable{object?})"/>
    public Task<int> ExecuteAsync(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => Execute(query, parameters));
    }
    /// <inheritdoc cref="RunInTransaction(Action)"/>
    public Task RunInTransactionAsync(Action action) {
        return Task.Run(() => RunInTransaction(action));
    }
    /// <inheritdoc cref="ExecuteScalar{T}(string, IEnumerable{object?})"/>
    public Task<T> ExecuteScalarAsync<T>(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => ExecuteScalar<T>(query, parameters));
    }
    /// <inheritdoc cref="Query(SqliteTable, string, IEnumerable{object?})"/>
    public Task<IEnumerable<object>> QueryAsync(SqliteTable map, string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => Query(map, query, parameters));
    }
    /// <inheritdoc cref="Query{T}(string, IEnumerable{object?})"/>
    public Task<IEnumerable<T>> QueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return Task.Run(() => Query<T>(query, parameters));
    }
    /// <inheritdoc cref="QueryScalars{T}(string, IEnumerable{object?})"/>
    public Task<List<T>> QueryScalarsAsync<T>(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => QueryScalars<T>(query, parameters));
    }
    /// <inheritdoc cref="ChangeKey(byte[])"/>
    public Task ChangeKeyAsync(byte[] key) {
        return Task.Run(() => ChangeKey(key));
    }
}