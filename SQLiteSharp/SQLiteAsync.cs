using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace SQLiteSharp;

public interface ISQLiteAsyncConnection {
    string DatabasePath { get; }
    int SQLiteVersionNumber { get; }
    bool Trace { get; set; }
    Action<string> Tracer { get; set; }
    bool TimeExecution { get; set; }
    IEnumerable<TableMapping> TableMappings { get; }

    Task BackupAsync(string destinationDatabasePath, string databaseName = "main");
    Task CloseAsync();
    Task<int> CreateIndexAsync(string tableName, string columnName, bool unique = false);
    Task<int> CreateIndexAsync(string indexName, string tableName, string columnName, bool unique = false);
    Task<int> CreateIndexAsync(string tableName, string[] columnNames, bool unique = false);
    Task<int> CreateIndexAsync(string indexName, string tableName, string[] columnNames, bool unique = false);
    Task<int> CreateIndexAsync<T>(Expression<Func<T, object>> property, bool unique = false);
    Task<CreateTableResult> CreateTableAsync<T>(CreateFlags createFlags = CreateFlags.None) where T : new();
    Task<CreateTableResult> CreateTableAsync(Type ty, CreateFlags createFlags = CreateFlags.None);
    Task<CreateTablesResult> CreateTablesAsync(CreateFlags createFlags = CreateFlags.None, params Type[] types);
    Task<IEnumerable<T>> DeferredQueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new();
    Task<IEnumerable<object>> DeferredQueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters);
    Task<int> DeleteAllAsync<T>();
    Task<int> DeleteAllAsync(TableMapping map);
    Task<int> DeleteAsync(object objectToDelete);
    Task<int> DeleteAsync<T>(object primaryKey);
    Task<int> DeleteAsync(object primaryKey, TableMapping map);
    Task<int> DropTableAsync<T>() where T : new();
    Task<int> DropTableAsync(TableMapping map);
    Task EnableLoadExtensionAsync(bool enabled);
    Task EnableWriteAheadLoggingAsync();
    Task<int> ExecuteAsync(string query, params IEnumerable<object?> parameters);
    Task<T> ExecuteScalarAsync<T>(string query, params IEnumerable<object?> parameters);
    Task<T?> FindAsync<T>(object pk) where T : new();
    Task<object?> FindAsync(object pk, TableMapping map);
    Task<T?> FindAsync<T>(Expression<Func<T, bool>> predicate) where T : new();
    Task<T?> FindWithQueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new();
    Task<object?> FindWithQueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters);
    Task<T> GetAsync<T>(object pk) where T : new();
    Task<object> GetAsync(object pk, TableMapping map);
    Task<T> GetAsync<T>(Expression<Func<T, bool>> predicate) where T : new();
    TimeSpan GetBusyTimeout();
    SQLiteConnectionWithLock GetConnection();
    Task<TableMapping> GetMappingAsync(Type type, CreateFlags createFlags = CreateFlags.None);
    Task<TableMapping> GetMappingAsync<T>(CreateFlags createFlags = CreateFlags.None) where T : new();
    Task<List<SQLiteConnection.ColumnInfo>> GetTableInfoAsync(string tableName);
    Task<int> InsertAsync(object obj, string? modifier = null);
    Task<int> InsertAllAsync(IEnumerable objects, string? modifier = null, bool runInTransaction = true);
    Task<int> InsertOrReplaceAsync(object obj);
    Task<int> InsertOrReplaceAllAsync(IEnumerable objects, bool runInTransaction = true);
    Task<int> InsertOrIgnoreAsync(object obj);
    Task<int> InsertOrIgnoreAllAsync(IEnumerable objects, bool runInTransaction = true);
    Task<List<T>> QueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new();
    Task<List<object>> QueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters);
    Task<List<T>> QueryScalarsAsync<T>(string query, params IEnumerable<object?> parameters);
    Task ReKeyAsync(string key);
    Task ReKeyAsync(byte[] key);
    Task RunInTransactionAsync(Action<SQLiteConnection> action);
    Task SetBusyTimeoutAsync(TimeSpan value);
    AsyncTableQuery<T> Table<T>() where T : new();
    Task<int> UpdateAllAsync(IEnumerable objects, bool runInTransaction = true);
    Task<int> UpdateAsync(object obj);
    Task<int> UpdateAsync(object obj, Type objType);
}

/// <summary>
/// A pooled asynchronous connection to a SQLite database.
/// </summary>
public partial class SQLiteAsyncConnection : ISQLiteAsyncConnection {
    private readonly SQLiteConnectionString _connectionString;

    /// <summary>
    /// Constructs a new SQLiteAsyncConnection and opens a pooled SQLite database specified by databasePath.
    /// </summary>
    /// <param name="databasePath">
    /// Specifies the path to the database file.
    /// </param>
    /// <param name="openFlags">
    /// Flags controlling how the connection should be opened.
    /// Async connections should have the FullMutex flag set to provide best performance.
    /// </param>
    public SQLiteAsyncConnection(string databasePath, SQLiteOpenFlags openFlags = SQLiteOpenFlags.Create | SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.FullMutex)
        : this(new SQLiteConnectionString(databasePath, openFlags)) {
    }

    /// <summary>
    /// Constructs a new SQLiteAsyncConnection and opens a pooled SQLite database using the given connection string.
    /// </summary>
    /// <param name="connectionString">
    /// Details on how to find and open the database.
    /// </param>
    public SQLiteAsyncConnection(SQLiteConnectionString connectionString) {
        _connectionString = connectionString;
    }

    /// <inheritdoc cref="SQLiteConnection.DatabasePath"/>
    public string DatabasePath => GetConnection().DatabasePath;
    /// <inheritdoc cref="SQLiteConnection.SQLiteVersionNumber"/>
    public int SQLiteVersionNumber => GetConnection().SQLiteVersionNumber;

    /// <summary>
    /// The amount of time to wait for a table to become unlocked.
    /// </summary>
    public TimeSpan GetBusyTimeout() {
        return GetConnection().BusyTimeout;
    }
    /// <summary>
    /// Sets the amount of time to wait for a table to become unlocked.
    /// </summary>
    public Task SetBusyTimeoutAsync(TimeSpan value) {
        return LockAsync(connection => {
            connection.BusyTimeout = value;
        });
    }
    /// <summary>
    /// Enables the write ahead logging. WAL is significantly faster in most scenarios
    /// by providing better concurrency and better disk IO performance than the normal
    /// journal mode. You only need to call this function once in the lifetime of the database.
    /// </summary>
    public Task EnableWriteAheadLoggingAsync() {
        return LockAsync(connection => {
            connection.EnableWriteAheadLogging();
        });
    }
    /// <summary>
    /// Whether to writer queries to <see cref="Tracer"/> during execution.
    /// </summary>
    public bool Trace {
        get => GetConnection().Trace;
        set => GetConnection().Trace = value;
    }
    /// <summary>
    /// The delegate responsible for writing trace lines.
    /// </summary>
    public Action<string> Tracer {
        get => GetConnection().Tracer;
        set => GetConnection().Tracer = value;
    }
    /// <summary>
    /// Whether Trace lines should be written that show the execution time of queries.
    /// </summary>
    public bool TimeExecution {
        get => GetConnection().TimeExecution;
        set => GetConnection().TimeExecution = value;
    }
    /// <summary>
    /// Returns the mappings from types to tables that the connection
    /// currently understands.
    /// </summary>
    public IEnumerable<TableMapping> TableMappings => GetConnection().TableMappings;
    /// <summary>
    /// Gets the pooled lockable connection used by this async connection.<br/>
    /// You should never need to use this. This is provided only to add additional functionality to SQLite-net.
    /// If you use this connection, you must use the Lock method on it while using it.
    /// </summary>
    public SQLiteConnectionWithLock GetConnection() {
        return SQLiteConnectionPool.Shared.GetConnection(_connectionString);
    }

    private SQLiteConnectionWithLock GetConnectionAndTransactionLock(out object transactionLock) {
        return SQLiteConnectionPool.Shared.GetConnectionAndTransactionLock(_connectionString, out transactionLock);
    }

    /// <summary>
    /// Closes any pooled connections used by the database.
    /// </summary>
    public Task CloseAsync() {
        return Task.Run(() => SQLiteConnectionPool.Shared.CloseConnection(_connectionString));
    }

    private Task<T> LockAsync<T>(Func<SQLiteConnectionWithLock, T> function) {
        return Task.Run(() => {
            SQLiteConnectionWithLock connection = GetConnection();
            using (connection.Lock()) {
                return function(connection);
            }
        });
    }
    private Task LockAsync(Action<SQLiteConnectionWithLock> action) {
        return Task.Run(() => {
            SQLiteConnectionWithLock connection = GetConnection();
            using (connection.Lock()) {
                action(connection);
            }
        });
    }
    private Task TransactAsync(Action<SQLiteConnectionWithLock> transact) {
        return Task.Run(() => {
            SQLiteConnectionWithLock connection = GetConnectionAndTransactionLock(out object transactionLock);
            lock (transactionLock) {
                using (connection.Lock()) {
                    transact(connection);
                }
            }
        });
    }

    /// <summary>
    /// Enable or disable extension loading.
    /// </summary>
    public Task EnableLoadExtensionAsync(bool enabled) {
        return LockAsync(connection => connection.EnableLoadExtension(enabled));
    }
    /// <summary>
    /// Executes a "create table if not exists" on the database. It also
    /// creates any specified indexes on the columns of the table. It uses
    /// a schema automatically generated from the specified type. You can
    /// later access this schema by calling GetMapping.
    /// </summary>
    /// <returns>
    /// Whether the table was created or migrated.
    /// </returns>
    public Task<CreateTableResult> CreateTableAsync<T>(CreateFlags createFlags = CreateFlags.None) where T : new() {
        return LockAsync(connection => connection.CreateTable<T>(createFlags));
    }
    /// <summary>
    /// Executes a "create table if not exists" on the database. It also
    /// creates any specified indexes on the columns of the table. It uses
    /// a schema automatically generated from the specified type. You can
    /// later access this schema by calling GetMapping.
    /// </summary>
    /// <param name="type">Type to reflect to a database table.</param>
    /// <param name="createFlags">Optional flags allowing implicit PK and indexes based on naming conventions.</param>  
    /// <returns>
    /// Whether the table was created or migrated.
    /// </returns>
    public Task<CreateTableResult> CreateTableAsync(Type type, CreateFlags createFlags = CreateFlags.None) {
        return LockAsync(connection => connection.CreateTable(type, createFlags));
    }
    /// <summary>
    /// Executes a "create table if not exists" on the database for each type. It also
    /// creates any specified indexes on the columns of the table. It uses
    /// a schema automatically generated from the specified type. You can
    /// later access this schema by calling GetMapping.
    /// </summary>
    /// <returns>
    /// Whether the table was created or migrated for each type.
    /// </returns>
    public Task<CreateTablesResult> CreateTablesAsync(CreateFlags createFlags = CreateFlags.None, params Type[] types) {
        return LockAsync(connection => connection.CreateTables(createFlags, types));
    }
    /// <summary>
    /// Executes a "drop table" on the database.  This is non-recoverable.
    /// </summary>
    public Task<int> DropTableAsync<T>() where T : new() {
        return LockAsync(connection => connection.DropTable<T>());
    }

    /// <summary>
    /// Executes a "drop table" on the database.  This is non-recoverable.
    /// </summary>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    public Task<int> DropTableAsync(TableMapping map) {
        return LockAsync(connection => connection.DropTable(map));
    }

    /// <summary>
    /// Creates an index for the specified table and column.
    /// </summary>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnName">Name of the column to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public Task<int> CreateIndexAsync(string tableName, string columnName, bool unique = false) {
        return LockAsync(connection => connection.CreateIndex(tableName, columnName, unique));
    }

    /// <summary>
    /// Creates an index for the specified table and column.
    /// </summary>
    /// <param name="indexName">Name of the index to create</param>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnName">Name of the column to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public Task<int> CreateIndexAsync(string indexName, string tableName, string columnName, bool unique = false) {
        return LockAsync(connection => connection.CreateIndex(indexName, tableName, columnName, unique));
    }

    /// <summary>
    /// Creates an index for the specified table and columns.
    /// </summary>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnNames">An array of column names to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public Task<int> CreateIndexAsync(string tableName, string[] columnNames, bool unique = false) {
        return LockAsync(connection => connection.CreateIndex(tableName, columnNames, unique));
    }

    /// <summary>
    /// Creates an index for the specified table and columns.
    /// </summary>
    /// <param name="indexName">Name of the index to create</param>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnNames">An array of column names to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public Task<int> CreateIndexAsync(string indexName, string tableName, string[] columnNames, bool unique = false) {
        return LockAsync(connection => connection.CreateIndex(indexName, tableName, columnNames, unique));
    }

    /// <summary>
    /// Creates an index for the specified object property.
    /// e.g. CreateIndex&lt;Client&gt;(c => c.Name);
    /// </summary>
    /// <typeparam name="T">Type to reflect to a database table.</typeparam>
    /// <param name="property">Property to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public Task<int> CreateIndexAsync<T>(Expression<Func<T, object>> property, bool unique = false) {
        return LockAsync(connection => connection.CreateIndex(property, unique));
    }

    /// <inheritdoc cref="SQLiteConnection.Insert(object, string?)"/>
    public Task<int> InsertAsync(object obj, string? modifier = null) {
        return LockAsync(connection => connection.Insert(obj, modifier));
    }
    /// <inheritdoc cref="SQLiteConnection.InsertAll(IEnumerable, string?, bool)"/>
    public Task<int> InsertAllAsync(IEnumerable objects, string? modifier = null, bool runInTransaction = true) {
        return LockAsync(connection => connection.InsertAll(objects, modifier, runInTransaction));
    }
    /// <inheritdoc cref="SQLiteConnection.InsertOrReplace(object)"/>
    public Task<int> InsertOrReplaceAsync(object obj) {
        return LockAsync(connection => connection.InsertOrReplace(obj));
    }
    /// <inheritdoc cref="SQLiteConnection.InsertOrReplaceAll(IEnumerable, bool)"/>
    public Task<int> InsertOrReplaceAllAsync(IEnumerable objects, bool runInTransaction = true) {
        return LockAsync(connection => connection.InsertOrReplaceAll(objects));
    }
    /// <inheritdoc cref="SQLiteConnection.InsertOrIgnore(object)"/>
    public Task<int> InsertOrIgnoreAsync(object obj) {
        return LockAsync(connection => connection.InsertOrIgnore(obj));
    }
    /// <inheritdoc cref="SQLiteConnection.InsertOrIgnoreAll(IEnumerable, bool)"/>
    public Task<int> InsertOrIgnoreAllAsync(IEnumerable objects, bool runInTransaction = true) {
        return LockAsync(connection => connection.InsertOrIgnoreAll(objects));
    }

    /// <summary>
    /// Updates all of the columns of a table using the specified object
    /// except for its primary key.
    /// The object is required to have a primary key.
    /// </summary>
    /// <param name="obj">
    /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
    /// </param>
    /// <returns>
    /// The number of rows updated.
    /// </returns>
    public Task<int> UpdateAsync(object obj) {
        return LockAsync(connection => connection.Update(obj));
    }

    /// <summary>
    /// Updates all of the columns of a table using the specified object
    /// except for its primary key.
    /// The object is required to have a primary key.
    /// </summary>
    /// <param name="obj">
    /// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
    /// </param>
    /// <param name="objType">
    /// The type of object to insert.
    /// </param>
    /// <returns>
    /// The number of rows updated.
    /// </returns>
    public Task<int> UpdateAsync(object obj, Type objType) {
        return LockAsync(connection => connection.Update(obj, objType));
    }

    /// <summary>
    /// Updates all specified objects.
    /// </summary>
    /// <param name="objects">
    /// An <see cref="IEnumerable"/> of the objects to insert.
    /// </param>
    /// <param name="runInTransaction">
    /// A boolean indicating if the inserts should be wrapped in a transaction
    /// </param>
    /// <returns>
    /// The number of rows modified.
    /// </returns>
    public Task<int> UpdateAllAsync(IEnumerable objects, bool runInTransaction = true) {
        return LockAsync(connection => connection.UpdateAll(objects, runInTransaction));
    }

    /// <summary>
    /// Deletes the given object from the database using its primary key.
    /// </summary>
    /// <param name="objectToDelete">
    /// The object to delete. It must have a primary key designated using the PrimaryKeyAttribute.
    /// </param>
    /// <returns>
    /// The number of rows deleted.
    /// </returns>
    public Task<int> DeleteAsync(object objectToDelete) {
        return LockAsync(connection => connection.Delete(objectToDelete));
    }

    /// <summary>
    /// Deletes the object with the specified primary key.
    /// </summary>
    /// <param name="primaryKey">
    /// The primary key of the object to delete.
    /// </param>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    /// <typeparam name='T'>
    /// The type of object.
    /// </typeparam>
    public Task<int> DeleteAsync<T>(object primaryKey) {
        return LockAsync(connection => connection.Delete<T>(primaryKey));
    }

    /// <summary>
    /// Deletes the object with the specified primary key.
    /// </summary>
    /// <param name="primaryKey">
    /// The primary key of the object to delete.
    /// </param>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public Task<int> DeleteAsync(object primaryKey, TableMapping map) {
        return LockAsync(connection => connection.Delete(primaryKey, map));
    }

    /// <summary>
    /// Deletes all the objects from the specified table.
    /// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
    /// specified table. Do you really want to do that?
    /// </summary>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    /// <typeparam name='T'>
    /// The type of objects to delete.
    /// </typeparam>
    public Task<int> DeleteAllAsync<T>() {
        return LockAsync(connection => connection.DeleteAll<T>());
    }

    /// <summary>
    /// Deletes all the objects from the specified table.
    /// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
    /// specified table. Do you really want to do that?
    /// </summary>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public Task<int> DeleteAllAsync(TableMapping map) {
        return LockAsync(connection => connection.DeleteAll(map));
    }

    /// <summary>
    /// Backup the entire database to the specified path.
    /// </summary>
    /// <param name="destinationDatabasePath">Path to backup file.</param>
    /// <param name="databaseName">The name of the database to backup (usually "main").</param>
    public Task BackupAsync(string destinationDatabasePath, string databaseName = "main") {
        return LockAsync(connection => {
            connection.Backup(destinationDatabasePath, databaseName);
            return 0;
        });
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="pk">
    /// The primary key.
    /// </param>
    /// <returns>
    /// The object with the given primary key. Throws a not found exception
    /// if the object is not found.
    /// </returns>
    public Task<T> GetAsync<T>(object pk) where T : new() {
        return LockAsync(connection => connection.Get<T>(pk));
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="pk">
    /// The primary key.
    /// </param>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The object with the given primary key. Throws a not found exception
    /// if the object is not found.
    /// </returns>
    public Task<object> GetAsync(object pk, TableMapping map) {
        return LockAsync(connection => connection.Get(pk, map));
    }

    /// <summary>
    /// Attempts to retrieve the first object that matches the predicate from the table
    /// associated with the specified type. 
    /// </summary>
    /// <param name="predicate">
    /// A predicate for which object to find.
    /// </param>
    /// <returns>
    /// The object that matches the given predicate. Throws a not found exception
    /// if the object is not found.
    /// </returns>
    public Task<T> GetAsync<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return LockAsync(connection => connection.Get<T>(predicate));
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="pk">
    /// The primary key.
    /// </param>
    /// <returns>
    /// The object with the given primary key or null
    /// if the object is not found.
    /// </returns>
    public Task<T?> FindAsync<T>(object pk) where T : new() {
        return LockAsync(connection => connection.Find<T>(pk));
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="pk">
    /// The primary key.
    /// </param>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The object with the given primary key or null
    /// if the object is not found.
    /// </returns>
    public Task<object?> FindAsync(object pk, TableMapping map) {
        return LockAsync(connection => connection.Find(pk, map));
    }

    /// <summary>
    /// Attempts to retrieve the first object that matches the predicate from the table
    /// associated with the specified type. 
    /// </summary>
    /// <param name="predicate">
    /// A predicate for which object to find.
    /// </param>
    /// <returns>
    /// The object that matches the given predicate or null
    /// if the object is not found.
    /// </returns>
    public Task<T?> FindAsync<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return LockAsync(connection => connection.Find(predicate));
    }

    /// <summary>
    /// Attempts to retrieve the first object that matches the query from the table
    /// associated with the specified type. 
    /// </summary>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// The object that matches the given predicate or null
    /// if the object is not found.
    /// </returns>
    public Task<T?> FindWithQueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return LockAsync(connection => connection.FindWithQuery<T>(query, parameters));
    }

    /// <summary>
    /// Attempts to retrieve the first object that matches the query from the table
    /// associated with the specified type. 
    /// </summary>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// The object that matches the given predicate or null
    /// if the object is not found.
    /// </returns>
    public Task<object?> FindWithQueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return LockAsync(connection => connection.FindWithQuery(map, query, parameters));
    }

    /// <summary>
    /// Retrieves the mapping that is automatically generated for the given type.
    /// </summary>
    /// <param name="type">
    /// The type whose mapping to the database is returned.
    /// </param>         
    /// <param name="createFlags">
    /// Optional flags allowing implicit PK and indexes based on naming conventions
    /// </param>     
    /// <returns>
    /// The mapping represents the schema of the columns of the database and contains 
    /// methods to set and get properties of objects.
    /// </returns>
    public Task<TableMapping> GetMappingAsync(Type type, CreateFlags createFlags = CreateFlags.None) {
        return LockAsync(connection => connection.GetMapping(type, createFlags));
    }

    /// <summary>
    /// Retrieves the mapping that is automatically generated for the given type.
    /// </summary>
    /// <param name="createFlags">
    /// Optional flags allowing implicit PK and indexes based on naming conventions
    /// </param>     
    /// <returns>
    /// The mapping represents the schema of the columns of the database and contains 
    /// methods to set and get properties of objects.
    /// </returns>
    public Task<TableMapping> GetMappingAsync<T>(CreateFlags createFlags = CreateFlags.None) where T : new() {
        return LockAsync(connection => connection.GetMapping<T>(createFlags));
    }

    /// <summary>
    /// Query the built-in sqlite table_info table for a specific tables columns.
    /// </summary>
    /// <returns>The columns contains in the table.</returns>
    /// <param name="tableName">Table name.</param>
    public Task<List<SQLiteConnection.ColumnInfo>> GetTableInfoAsync(string tableName) {
        return LockAsync(connection => connection.GetTableInfo(tableName));
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// Use this method instead of Query when you don't expect rows back. Such cases include
    /// INSERTs, UPDATEs, and DELETEs.
    /// You can set the Trace or TimeExecution properties of the connection
    /// to profile execution.
    /// </summary>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// The number of rows modified in the database as a result of this execution.
    /// </returns>
    public Task<int> ExecuteAsync(string query, params IEnumerable<object?> parameters) {
        return LockAsync(connection => connection.Execute(query, parameters));
    }

    /// <summary>
    /// Inserts all specified objects.
    /// </summary>
    /// <param name="objects">
    /// An <see cref="IEnumerable"/> of the objects to insert.
    /// <param name="runInTransaction"/>
    /// A boolean indicating if the inserts should be wrapped in a transaction.
    /// </param>
    /// <returns>
    /// The number of rows added to the table.
    /// </returns>
    public Task<int> InsertAllAsync(IEnumerable objects, bool runInTransaction = true) {
        return LockAsync(connection => connection.InsertAll(objects, runInTransaction));
    }

    /// <summary>
    /// Inserts all specified objects.
    /// </summary>
    /// <param name="objects">
    /// An <see cref="IEnumerable"/> of the objects to insert.
    /// </param>
    /// <param name="extra">
    /// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
    /// </param>
    /// <param name="runInTransaction">
    /// A boolean indicating if the inserts should be wrapped in a transaction.
    /// </param>
    /// <returns>
    /// The number of rows added to the table.
    /// </returns>
    public Task<int> InsertAllAsync(IEnumerable objects, string modifier, bool runInTransaction = true) {
        return LockAsync(connection => connection.InsertAll(objects, modifier, runInTransaction));
    }

    /// <summary>
    /// Inserts all specified objects.
    /// </summary>
    /// <param name="objects">
    /// An <see cref="IEnumerable"/> of the objects to insert.
    /// </param>
    /// <param name="objType">
    /// The type of object to insert.
    /// </param>
    /// <param name="runInTransaction">
    /// A boolean indicating if the inserts should be wrapped in a transaction.
    /// </param>
    /// <returns>
    /// The number of rows added to the table.
    /// </returns>
    public Task<int> InsertAllAsync(IEnumerable objects, Type objType, bool runInTransaction = true) {
        return LockAsync(connection => connection.InsertAll(objects, objType, runInTransaction));
    }

    /// <summary>
    /// Executes <paramref name="action"/> within a (possibly nested) transaction by wrapping it in a SAVEPOINT. If an
    /// exception occurs the whole transaction is rolled back, not just the current savepoint. The exception
    /// is rethrown.
    /// </summary>
    /// <param name="action">
    /// The <see cref="Action"/> to perform within a transaction. <paramref name="action"/> can contain any number
    /// of operations on the connection but should never call <see cref="SQLiteConnection.Commit"/> or
    /// <see cref="SQLiteConnection.Commit"/>.
    /// </param>
    public Task RunInTransactionAsync(Action<SQLiteConnection> action) {
        return TransactAsync(connection => {
            connection.BeginTransaction();
            try {
                action(connection);
                connection.Commit();
            }
            finally {
                connection.Rollback();
            }
        });
    }

    /// <summary>
    /// Returns a queryable interface to the table represented by the given type.
    /// </summary>
    /// <remarks>
    /// This function is synchronous because the database isn't touched until a query is performed.
    /// </remarks>
    /// <returns>
    /// A queryable object that is able to translate Where, OrderBy, and Take queries into native SQL.
    /// </returns>
    public AsyncTableQuery<T> Table<T>() where T : new() {
        SQLiteConnectionWithLock connection = GetConnection();
        return new AsyncTableQuery<T>(connection.Table<T>());
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// Use this method when return primitive values.
    /// You can set the Trace or TimeExecution properties of the connection
    /// to profile execution.
    /// </summary>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// The number of rows modified in the database as a result of this execution.
    /// </returns>
    public Task<T> ExecuteScalarAsync<T>(string query, params IEnumerable<object?> parameters) {
        return LockAsync(connection => {
            SQLiteCommand command = connection.CreateCommand(query, parameters);
            return command.ExecuteScalar<T>();
        });
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// It returns each row of the result using the mapping automatically generated for
    /// the given type.
    /// </summary>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// A list with one result for each row returned by the query.
    /// </returns>
    public Task<List<T>> QueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return LockAsync(connection => connection.Query<T>(query, parameters));
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
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
    /// A list with one result for the first column of each row returned by the query.
    /// </returns>
    public Task<List<T>> QueryScalarsAsync<T>(string query, params IEnumerable<object?> parameters) {
        return LockAsync(connection => connection.QueryScalars<T>(query, parameters));
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// It returns each row of the result using the specified mapping. This function is
    /// only used by libraries in order to query the database via introspection. It is
    /// normally not used.
    /// </summary>
    /// <param name="map">
    /// A <see cref="TableMapping"/> to use to convert the resulting rows
    /// into objects.
    /// </param>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// An enumerable with one result for each row returned by the query.
    /// </returns>
    public Task<List<object>> QueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return LockAsync(connection => connection.Query(map, query, parameters));
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// It returns each row of the result using the mapping automatically generated for
    /// the given type.
    /// </summary>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// An enumerable with one result for each row returned by the query.
    /// The enumerator will call sqlite3_step on each call to MoveNext, so the database
    /// connection must remain open for the lifetime of the enumerator.
    /// </returns>
    public Task<IEnumerable<T>> DeferredQueryAsync<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return LockAsync(connection => (IEnumerable<T>)connection.DeferredQuery<T>(query, parameters).ToList());
    }

    /// <summary>
    /// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
    /// in the command text for each of the arguments and then executes that command.
    /// It returns each row of the result using the specified mapping. This function is
    /// only used by libraries in order to query the database via introspection. It is
    /// normally not used.
    /// </summary>
    /// <param name="map">
    /// A <see cref="TableMapping"/> to use to convert the resulting rows
    /// into objects.
    /// </param>
    /// <param name="query">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the query.
    /// </param>
    /// <returns>
    /// An enumerable with one result for each row returned by the query.
    /// The enumerator will call sqlite3_step on each call to MoveNext, so the database
    /// connection must remain open for the lifetime of the enumerator.
    /// </returns>
    public Task<IEnumerable<object>> DeferredQueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return LockAsync(connection => (IEnumerable<object>)connection.DeferredQuery(map, query, parameters).ToList());
    }

    /// <summary>
    /// Change the encryption key for a SQLCipher database with "pragma rekey = ...".
    /// </summary>
    /// <param name="key">Encryption key plain text that is converted to the real encryption key using PBKDF2 key derivation</param>
    public Task ReKeyAsync(string key) {
        return LockAsync(connection => connection.ReKey(key));
    }

    /// <summary>
    /// Change the encryption key for a SQLCipher database.
    /// </summary>
    /// <param name="key">256-bit (32 byte) or 384-bit (48 bytes) encryption key data</param>
    public Task ReKeyAsync(byte[] key) {
        return LockAsync(connection => connection.ReKey(key));
    }
}

/// <summary>
/// Query to an asynchronous database connection.
/// </summary>
public class AsyncTableQuery<T> where T : new() {
    private readonly TableQuery<T> _query;

    /// <summary>
    /// Creates a new async query that uses given the synchronous query.
    /// </summary>
    public AsyncTableQuery(TableQuery<T> query) {
        _query = query;
    }

    private Task<T2> LockAsync<T2>(Func<SQLiteConnectionWithLock, T2> function) {
        return Task.Run(() => {
            SQLiteConnectionWithLock connection = (SQLiteConnectionWithLock)_query.Connection;
            using (connection.Lock()) {
                return function(connection);
            }
        });
    }

    /// <summary>
    /// Filters the query based on a predicate.
    /// </summary>
    public AsyncTableQuery<T> Where(Expression<Func<T, bool>> predExpr) {
        return new AsyncTableQuery<T>(_query.Where(predExpr));
    }

    /// <summary>
    /// Skips a given number of elements from the query and then yields the remainder.
    /// </summary>
    public AsyncTableQuery<T> Skip(int n) {
        return new AsyncTableQuery<T>(_query.Skip(n));
    }

    /// <summary>
    /// Yields a given number of elements from the query and then skips the remainder.
    /// </summary>
    public AsyncTableQuery<T> Take(int n) {
        return new AsyncTableQuery<T>(_query.Take(n));
    }

    /// <summary>
    /// Order the query results according to a key.
    /// </summary>
    public AsyncTableQuery<T> OrderBy<U>(Expression<Func<T, U>> orderExpr) {
        return new AsyncTableQuery<T>(_query.OrderBy(orderExpr));
    }

    /// <summary>
    /// Order the query results according to a key.
    /// </summary>
    public AsyncTableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> orderExpr) {
        return new AsyncTableQuery<T>(_query.OrderByDescending(orderExpr));
    }

    /// <summary>
    /// Queries the database and returns the results as a List.
    /// </summary>
    public Task<List<T>> ToListAsync() {
        return LockAsync(connection => _query.ToList());
    }

    /// <summary>
    /// Queries the database and returns the results as an array.
    /// </summary>
    public Task<T[]> ToArrayAsync() {
        return LockAsync(connection => _query.ToArray());
    }

    /// <summary>
    /// Execute SELECT COUNT(*) on the query
    /// </summary>
    public Task<int> CountAsync() {
        return LockAsync(connection => _query.Count());
    }

    /// <summary>
    /// Execute SELECT COUNT(*) on the query with an additional WHERE clause.
    /// </summary>
    public Task<int> CountAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.Count(predExpr));
    }

    /// <summary>
    /// Returns the element at a given index
    /// </summary>
    public Task<T> ElementAtAsync(int index) {
        return LockAsync(connection => _query.ElementAt(index));
    }

    /// <summary>
    /// Returns the first element of this query.
    /// </summary>
    public Task<T> FirstAsync() {
        return LockAsync(connection => _query.First());
    }

    /// <summary>
    /// Returns the first element of this query, or null if no element is found.
    /// </summary>
    public Task<T?> FirstOrDefaultAsync() {
        return LockAsync(connection => _query.FirstOrDefault());
    }

    /// <summary>
    /// Returns the first element of this query that matches the predicate.
    /// </summary>
    public Task<T> FirstAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.First(predExpr));
    }

    /// <summary>
    /// Returns the first element of this query that matches the predicate.
    /// </summary>
    public Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.FirstOrDefault(predExpr));
    }

    /// <summary>
    /// Delete all the rows that match this query and the given predicate.
    /// </summary>
    public Task<int> DeleteAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.Delete(predExpr));
    }

    /// <summary>
    /// Delete all the rows that match this query.
    /// </summary>
    public Task<int> DeleteAsync() {
        return LockAsync(connection => _query.Delete());
    }
}

internal class SQLiteConnectionPool {
    private class Entry {
        public SQLiteConnectionWithLock? Connection { get; private set; }
        public SQLiteConnectionString ConnectionString { get; }
        public object TransactionLock { get; } = new object();

        public Entry(SQLiteConnectionString connectionString) {
            ConnectionString = connectionString;
            Connection = new SQLiteConnectionWithLock(ConnectionString);

            // If the database is FullMutex, don't bother locking
            if (ConnectionString.OpenFlags.HasFlag(SQLiteOpenFlags.FullMutex)) {
                Connection.SkipLock = true;
            }
        }

        public void Close() {
            SQLiteConnectionWithLock? connection = Connection;
            Connection = null;
            connection?.Close();
        }
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = [];

    /// <summary>
    /// Gets the singleton instance of the connection tool.
    /// </summary>
    public static SQLiteConnectionPool Shared { get; } = new();

    public SQLiteConnectionWithLock GetConnection(SQLiteConnectionString connectionString) {
        return GetConnectionAndTransactionLock(connectionString, out var _);
    }
    public SQLiteConnectionWithLock GetConnectionAndTransactionLock(SQLiteConnectionString connectionString, out object transactionLock) {
        Entry entry = _entries.GetOrAdd(connectionString.UniqueKey, key => new Entry(connectionString));
        transactionLock = entry.TransactionLock;
        return entry.Connection!;
    }
    public void CloseConnection(SQLiteConnectionString connectionString) {
        if (_entries.TryRemove(connectionString.UniqueKey, out Entry? entry)) {
            entry.Close();
        }
    }
}

/// <summary>
/// This is a normal connection except it contains a Lock method that can be used to serialize access to the database in lieu of using the sqlite's FullMutex support.
/// </summary>
public class SQLiteConnectionWithLock : SQLiteConnection {
    private readonly object _lock = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="T:SQLite.SQLiteConnectionWithLock"/> class.
    /// </summary>
    /// <param name="connectionString">Connection string containing the DatabasePath.</param>
    public SQLiteConnectionWithLock(SQLiteConnectionString connectionString)
        : base(connectionString) {
    }

    /// <summary>
    /// Whether to skip using the monitor lock (used when <see cref="SQLiteOpenFlags.FullMutex"/> is enabled).
    /// </summary>
    public bool SkipLock { get; set; }

    /// <summary>
    /// Lock the database to serialize access to it. To unlock it, dispose on the returned object.
    /// </summary>
    public IDisposable Lock() {
        return SkipLock ? new FakeLockWrapper() : new LockWrapper(_lock);
    }

    private class LockWrapper : IDisposable {
        private readonly object _lock;

        public LockWrapper(object @lock) {
            _lock = @lock;
            Monitor.Enter(_lock);
        }
        public void Dispose() {
            Monitor.Exit(_lock);
        }
    }
    private class FakeLockWrapper : IDisposable {
        public void Dispose() {
        }
    }
}