using System.Collections;
using System.Text;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

using Sqlite3 = SQLitePCL.raw;
using Sqlite3BackupHandle = SQLitePCL.sqlite3_backup;
using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;
using Sqlite3Statement = SQLitePCL.sqlite3_stmt;

namespace SQLiteSharp;

public class SQLiteException(SQLite3.Result result, string message) : Exception(message) {
    public SQLite3.Result Result { get; } = result;
}
public class NotNullConstraintViolationException : SQLiteException {
    public IEnumerable<TableMapping.Column>? Columns { get; }

    public NotNullConstraintViolationException(SQLite3.Result result, string message, TableMapping? mapping, object? obj)
        : base(result, message) {
        if (mapping is not null && obj is not null) {
            Columns = mapping.Columns.Where(column => !column.IsNullable && column.GetValue(obj) is null);
        }
    }
    public NotNullConstraintViolationException(SQLite3.Result result, string message)
        : this(result, message, null, null) {
    }
    public NotNullConstraintViolationException(SQLiteException exception, TableMapping mapping, object obj)
        : this(exception.Result, exception.Message, mapping, obj) {
    }
}

[Flags]
public enum SQLiteOpenFlags {
    ReadOnly = 1, ReadWrite = 2, Create = 4,
    Uri = 0x40, Memory = 0x80,
    NoMutex = 0x8000, FullMutex = 0x10000,
    SharedCache = 0x20000, PrivateCache = 0x40000,
    ProtectionComplete = 0x00100000,
    ProtectionCompleteUnlessOpen = 0x00200000,
    ProtectionCompleteUntilFirstUserAuthentication = 0x00300000,
    ProtectionNone = 0x00400000
}

[Flags]
public enum CreateFlags {
    /// <summary>
    /// Use the default creation options
    /// </summary>
    None = 0x000,
    /// <summary>
    /// Create a primary key index for a property called 'Id' (case-insensitive).
    /// This avoids the need for the [PrimaryKey] attribute.
    /// </summary>
    ImplicitPrimaryKey = 0x001,
    /// <summary>
    /// Create indices for properties ending in 'Id' (case-insensitive).
    /// </summary>
    ImplicitIndex = 0x002,
    /// <summary>
    /// Create a primary key for a property called 'Id' and
    /// create an indices for properties ending in 'Id' (case-insensitive).
    /// </summary>
    AllImplicit = 0x003,
    /// <summary>
    /// Force the primary key property to be auto incrementing.
    /// This avoids the need for the [AutoIncrement] attribute.
    /// The primary key property on the class should have type int or long.
    /// </summary>
    AutoIncrementPrimaryKey = 0x004,
    /// <summary>
    /// Create virtual table using FTS3
    /// </summary>
    FullTextSearch3 = 0x100,
    /// <summary>
    /// Create virtual table using FTS4
    /// </summary>
    FullTextSearch4 = 0x200
}

public interface ISQLiteConnection : IDisposable {
    Sqlite3DatabaseHandle? Handle { get; }
    string DatabasePath { get; }
    int SQLiteVersionNumber { get; }
    bool TimeExecution { get; set; }
    bool Trace { get; set; }
    Action<string> Tracer { get; set; }
    TimeSpan BusyTimeout { get; set; }
    IEnumerable<TableMapping> TableMappings { get; }
    bool IsInTransaction { get; }

    event EventHandler<NotifyTableChangedEventArgs>? TableChanged;

    void Backup(string destinationDatabasePath, string databaseName = "main");
    void BeginTransaction();
    void Close();
    void Commit();
    SQLiteCommand CreateCommand(string commandText, params IEnumerable<object?> parameters);
    SQLiteCommand CreateCommand(string commandText, Dictionary<string, object> parameters);
    int CreateIndex(string indexName, string tableName, string[] columnNames, bool unique = false);
    int CreateIndex(string indexName, string tableName, string columnName, bool unique = false);
    int CreateIndex(string tableName, string columnName, bool unique = false);
    int CreateIndex(string tableName, string[] columnNames, bool unique = false);
    int CreateIndex<T>(Expression<Func<T, object>> property, bool unique = false);
    CreateTableResult CreateTable<T>(CreateFlags createFlags = CreateFlags.None);
    CreateTableResult CreateTable(Type type, CreateFlags createFlags = CreateFlags.None);
    CreateTablesResult CreateTables(CreateFlags createFlags = CreateFlags.None, params IEnumerable<Type> types);
    IEnumerable<T> DeferredQuery<T>(string query, params IEnumerable<object?> parameters) where T : new();
    IEnumerable<object> DeferredQuery(TableMapping map, string query, params IEnumerable<object?> parameters);
    int Delete(object objectToDelete);
    int Delete<T>(object primaryKey);
    int Delete(object primaryKey, TableMapping map);
    int DeleteAll<T>();
    int DeleteAll(TableMapping map);
    int DropTable<T>();
    int DropTable(TableMapping map);
    void EnableLoadExtension(bool enabled);
    void EnableWriteAheadLogging();
    int Execute(string query, params IEnumerable<object?> parameters);
    T ExecuteScalar<T>(string query, params IEnumerable<object?> parameters);
    T? Find<T>(object primaryKey) where T : new();
    object? Find(object primaryKey, TableMapping map);
    T? Find<T>(Expression<Func<T, bool>> predicate) where T : new();
    T? FindWithQuery<T>(string query, params IEnumerable<object?> parameters) where T : new();
    object? FindWithQuery(TableMapping map, string query, params IEnumerable<object?> parameters);
    T Get<T>(object primaryKey) where T : new();
    object Get(object primaryKey, TableMapping map);
    T Get<T>(Expression<Func<T, bool>> predicate) where T : new();
    TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None);
    TableMapping GetMapping<T>(CreateFlags createFlags = CreateFlags.None);
    List<SQLiteConnection.ColumnInfo> GetTableInfo(string tableName);
    int Insert(object obj, string? modifier = null);
    int InsertAll(IEnumerable objects, string? modifier = null, bool runInTransaction = true);
    int InsertOrReplace(object obj);
    int InsertOrReplaceAll(IEnumerable objects, bool runInTransaction = true);
    int InsertOrIgnore(object obj);
    int InsertOrIgnoreAll(IEnumerable objects, bool runInTransaction = true);
    List<T> Query<T>(string query, params IEnumerable<object?> parameters) where T : new();
    List<object> Query(TableMapping map, string query, params IEnumerable<object?> parameters);
    List<T> QueryScalars<T>(string query, params IEnumerable<object?> parameters);
    void ReKey(string key);
    void ReKey(byte[] key);
    void Release(string savepoint);
    void Rollback();
    void RollbackTo(string savepoint);
    void RunInTransaction(Action action);
    string SaveTransactionPoint();
    TableQuery<T> Table<T>() where T : new();
    int Update(object obj);
    int Update(object obj, Type objType);
    int UpdateAll(IEnumerable objects, bool runInTransaction = true);
}

/// <summary>
/// An open connection to a SQLite database.
/// </summary>
public partial class SQLiteConnection : ISQLiteConnection {
    private readonly static Dictionary<string, TableMapping> _mappings = [];

    private bool _open;
    private TimeSpan _busyTimeout;
    private Stopwatch? _stopwatch;
    private long _elapsedMilliseconds = 0;

    private int _transactionDepth = 0;
    private readonly Random _rand = new();

    public Sqlite3DatabaseHandle? Handle { get; private set; }

    /// <summary>
    /// The database path used by this connection.
    /// </summary>
    public string DatabasePath { get; }
    /// <summary>
    /// The SQLite library version number. <c>3007014</c> refers to <c>v3.7.14</c>.
    /// </summary>
    public int SQLiteVersionNumber { get; }
    /// <summary>
    /// Whether Trace lines should be written that show the execution time of queries.
    /// </summary>
    public bool TimeExecution { get; set; }
    /// <summary>
    /// Whether to write queries to <see cref="Tracer"/> during execution.
    /// </summary>
    public bool Trace { get; set; }
    /// <summary>
    /// The delegate responsible for writing trace lines.
    /// </summary>
    /// <value>The tracer.</value>
    public Action<string> Tracer { get; set; }

    static SQLiteConnection() {
		SQLitePCL.Batteries_V2.Init();
    }

    /// <summary>
    /// Constructs a new SQLiteConnection and opens a SQLite database specified by databasePath.
    /// </summary>
    /// <param name="databasePath">
    /// Specifies the path to the database file.
    /// </param>
    /// <param name="openFlags">
    /// Flags controlling how the connection should be opened.
    /// </param>
    public SQLiteConnection(string databasePath, SQLiteOpenFlags openFlags = SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create)
        : this(new SQLiteConnectionString(databasePath, openFlags)) {
    }

    /// <summary>
    /// Constructs a new SQLiteConnection and opens a SQLite database specified by databasePath.
    /// </summary>
    /// <param name="connectionString">
    /// Details on how to find and open the database.
    /// </param>
    public SQLiteConnection(SQLiteConnectionString connectionString) {
        if (connectionString.DatabasePath is null) {
            throw new InvalidOperationException("DatabasePath must be specified");
        }

        DatabasePath = connectionString.DatabasePath;

        SQLiteVersionNumber = SQLite3.LibVersionNumber();

        SQLite3.Result result = SQLite3.Open(connectionString.DatabasePath, out Sqlite3DatabaseHandle handle, (int)connectionString.OpenFlags, connectionString.VfsName);
        Handle = handle;

        if (result is not SQLite3.Result.OK) {
            throw new SQLiteException(result, $"Could not open database file: {DatabasePath} ({result})");
        }
        _open = true;

        BusyTimeout = TimeSpan.FromSeconds(1.0);
        Tracer = line => Debug.WriteLine(line);

        connectionString.PreKeyAction?.Invoke(this);
        if (connectionString.Key is string stringKey) {
            SetKey(stringKey);
        }
        else if (connectionString.Key is byte[] bytesKey) {
            SetKey(bytesKey);
        }
        else if (connectionString.Key is not null) {
            throw new InvalidOperationException("Encryption key must be string or byte array");
        }
        connectionString.PostKeyAction?.Invoke(this);
    }

    /// <summary>
    /// Enables the write ahead logging. WAL is significantly faster in most scenarios
    /// by providing better concurrency and better disk IO performance than the normal
    /// journal mode. You only need to call this function once in the lifetime of the database.
    /// </summary>
    public void EnableWriteAheadLogging() {
        ExecuteScalar<string>("PRAGMA journal_mode=WAL");
    }

    /// <summary>
    /// Convert an input string to a quoted SQL string that can be safely used in queries.
    /// </summary>
    /// <returns>The quoted string.</returns>
    /// <param name="unsafeString">The unsafe string to quote.</param>
    static string Quote(string? unsafeString) {
        // TODO: Doesn't call sqlite3_mprintf("%Q", u) because we're waiting on https://github.com/ericsink/SQLitePCL.raw/issues/153
        if (unsafeString is null) {
            return "NULL";
        }
        string safe = unsafeString.Replace("'", "''");
        return "'" + safe + "'";
    }

    /// <summary>
    /// Sets the key used to encrypt/decrypt the database with "pragma key = ...".
    /// This must be the first thing you call before doing anything else with this connection
    /// if your database is encrypted.
    /// This only has an effect if you are using the SQLCipher nuget package.
    /// </summary>
    /// <param name="key">Encryption key plain text that is converted to the real encryption key using PBKDF2 key derivation</param>
    void SetKey(string key) {
        string quotedKey = Quote(key);
        ExecuteScalar<string>("pragma key = " + quotedKey);
    }

    /// <summary>
    /// Sets the key used to encrypt/decrypt the database.
    /// This must be the first thing you call before doing anything else with this connection
    /// if your database is encrypted.
    /// This only has an effect if you are using the SQLCipher nuget package.
    /// </summary>
    /// <param name="key">256-bit (32 byte) encryption key data</param>
    void SetKey(byte[] key) {
        if (key.Length != 32 && key.Length != 48)
            throw new ArgumentException("Key must be 32 bytes (256-bit) or 48 bytes (384-bit)", nameof(key));
        string keyHexString = string.Concat(key.Select(x => x.ToString("X2")));
        ExecuteScalar<string>("pragma key = \"x'" + keyHexString + "'\"");
    }

    /// <summary>
    /// Change the encryption key for a SQLCipher database with "pragma rekey = ...".
    /// </summary>
    /// <param name="key">Encryption key plain text that is converted to the real encryption key using PBKDF2 key derivation</param>
    public void ReKey(string key) {
        var quotedKey = Quote(key);
        ExecuteScalar<string>("pragma rekey = " + quotedKey);
    }

    /// <summary>
    /// Change the encryption key for a SQLCipher database.
    /// </summary>
    /// <param name="key">256-bit (32 byte) or 384-bit (48 bytes) encryption key data</param>
    public void ReKey(byte[] key) {
        if (key.Length != 32 && key.Length != 48)
            throw new ArgumentException("Key must be 32 bytes (256-bit) or 48 bytes (384-bit)", nameof(key));
        string keyHexString = string.Concat(key.Select(x => x.ToString("X2")));
        ExecuteScalar<string>("pragma rekey = \"x'" + keyHexString + "'\"");
    }

    /// <summary>
    /// Enable or disable extension loading.
    /// </summary>
    public void EnableLoadExtension(bool enabled) {
        SQLite3.Result result = SQLite3.EnableLoadExtension(Handle!, enabled ? 1 : 0);
        if (result != SQLite3.Result.OK) {
            string errorMessage = SQLite3.GetErrmsg(Handle!);
            throw new SQLiteException(result, errorMessage);
        }
    }

    /// <summary>
    /// Sets a busy handler to sleep the specified amount of time when a table is locked.
    /// The handler will sleep multiple times until a total time of <see cref="BusyTimeout"/> has accumulated.
    /// </summary>
    public TimeSpan BusyTimeout {
        get => _busyTimeout;
        set {
            _busyTimeout = value;
            if (Handle is not null) {
                SQLite3.BusyTimeout(Handle, (int)_busyTimeout.TotalMilliseconds);
            }
        }
    }

    /// <summary>
    /// Returns the mappings from types to tables that the connection
    /// currently understands.
    /// </summary>
    public IEnumerable<TableMapping> TableMappings {
        get {
            lock (_mappings) {
                return new List<TableMapping>(_mappings.Values);
            }
        }
    }

    /// <summary>
    /// Retrieves the mapping that is automatically generated for the given type.
    /// </summary>
    /// <param name="type">
    /// The type whose mapping to the database is returned.
    /// </param>
    /// <param name="createFlags">
    /// Optional flags allowing implicit primary key and indexes based on naming conventions
    /// </param>
    /// <returns>
    /// The mapping represents the schema of the columns of the database and contains
    /// methods to set and get properties of objects.
    /// </returns>
    public TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None) {
        string key = type.FullName ?? throw new ArgumentException("Type must have a fully qualified name");
        lock (_mappings) {
            if (_mappings.TryGetValue(key, out TableMapping? map)) {
                if (createFlags != CreateFlags.None && createFlags != map.CreateFlags) {
                    map = new TableMapping(type, createFlags);
                    _mappings[key] = map;
                }
            }
            else {
                map = new TableMapping(type, createFlags);
                _mappings.Add(key, map);
            }
            return map;
        }
    }

    /// <summary>
    /// Retrieves the mapping that is automatically generated for the given type.
    /// </summary>
    /// <param name="createFlags">
    /// Optional flags allowing implicit primary key and indexes based on naming conventions
    /// </param>
    /// <returns>
    /// The mapping represents the schema of the columns of the database and contains
    /// methods to set and get properties of objects.
    /// </returns>
    public TableMapping GetMapping<T>(CreateFlags createFlags = CreateFlags.None) {
        return GetMapping(typeof(T), createFlags);
    }

    private struct IndexedColumn {
        public int Order;
        public string ColumnName;
    }

    private struct IndexInfo {
        public string IndexName;
        public string TableName;
        public bool Unique;
        public List<IndexedColumn> Columns;
    }

    /// <summary>
    /// Executes a "drop table" on the database. This is non-recoverable.
    /// </summary>
    public int DropTable<T>() {
        return DropTable(GetMapping<T>());
    }

    /// <summary>
    /// Executes a "drop table" on the database. This is non-recoverable.
    /// </summary>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    public int DropTable(TableMapping map) {
        string query = $"drop table if exists \"{map.TableName}\"";
        return Execute(query);
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
    public CreateTableResult CreateTable<T>(CreateFlags createFlags = CreateFlags.None) {
        return CreateTable(typeof(T), createFlags);
    }

    /// <summary>
    /// Executes a "create table if not exists" on the database. It also
    /// creates any specified indexes on the columns of the table. It uses
    /// a schema automatically generated from the specified type. You can
    /// later access this schema by calling GetMapping.
    /// </summary>
    /// <param name="type">Type to reflect to a database table.</param>
    /// <param name="createFlags">Optional flags allowing implicit primary key and indexes based on naming conventions.</param>
    /// <returns>
    /// Whether the table was created or migrated.
    /// </returns>
    public CreateTableResult CreateTable(Type type, CreateFlags createFlags = CreateFlags.None) {
        TableMapping map = GetMapping(type, createFlags);

        // Present a nice error if no columns specified
        if (map.Columns.Length == 0) {
            throw new Exception($"Cannot create a table without columns (does '{type.FullName}' have public properties?)");
        }

        // Check if the table exists
        CreateTableResult result = CreateTableResult.Created;
        List<ColumnInfo> existingCols = GetTableInfo(map.TableName);

        // Create or migrate it
        if (existingCols.Count == 0) {
            // Facilitate virtual tables a.k.a. full-text search.
            bool fts3 = (createFlags & CreateFlags.FullTextSearch3) != 0;
            bool fts4 = (createFlags & CreateFlags.FullTextSearch4) != 0;
            bool fts = fts3 || fts4;
            string @virtual = fts ? "virtual " : string.Empty;
            string @using = fts3 ? "using fts3 " : fts4 ? "using fts4 " : string.Empty;

            // Build query.
            string query = "create " + @virtual + "table if not exists \"" + map.TableName + "\" " + @using + "(\n";
            var decls = map.Columns.Select(Orm.SqlDecl);
            var decl = string.Join(",\n", decls.ToArray());
            query += decl;
            query += ")";
            if (map.WithoutRowId) {
                query += " without rowid";
            }

            Execute(query);
        }
        else {
            result = CreateTableResult.Migrated;
            MigrateTable(map, existingCols);
        }

        Dictionary<string, IndexInfo> indexes = [];
        foreach (TableMapping.Column column in map.Columns) {
            foreach (IndexedAttribute index in column.Indices) {
                string indexName = index.Name ?? map.TableName + "_" + column.Name;
                if (!indexes.TryGetValue(indexName, out IndexInfo indexInfo)) {
                    indexInfo = new IndexInfo() {
                        IndexName = indexName,
                        TableName = map.TableName,
                        Unique = index.Unique,
                        Columns = []
                    };
                    indexes.Add(indexName, indexInfo);
                }

                if (index.Unique != indexInfo.Unique) {
                    throw new Exception("All the columns in an index must have the same value for their Unique property");
                }

                indexInfo.Columns.Add(new IndexedColumn {
                    Order = index.Order,
                    ColumnName = column.Name
                });
            }
        }

        foreach (string indexName in indexes.Keys) {
            IndexInfo index = indexes[indexName];
            string[] columns = index.Columns.OrderBy(i => i.Order).Select(i => i.ColumnName).ToArray();
            CreateIndex(indexName, index.TableName, columns, index.Unique);
        }

        return result;
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
    public CreateTablesResult CreateTables(CreateFlags createFlags = CreateFlags.None, params IEnumerable<Type> types) {
        CreateTablesResult result = new();
        foreach (Type type in types) {
            CreateTableResult oneResult = CreateTable(type, createFlags);
            result.Results[type] = oneResult;
        }
        return result;
    }

    /// <summary>
    /// Creates an index for the specified table and columns.
    /// </summary>
    /// <param name="indexName">Name of the index to create</param>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnNames">An array of column names to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public int CreateIndex(string indexName, string tableName, string[] columnNames, bool unique = false) {
        string sql = $"create {(unique ? "unique" : "")} index if not exists \"{indexName}\" on \"{tableName}\"(\"{string.Join("\", \"", columnNames)}\")";
        return Execute(sql);
    }

    /// <summary>
    /// Creates an index for the specified table and column.
    /// </summary>
    /// <param name="indexName">Name of the index to create</param>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnName">Name of the column to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public int CreateIndex(string indexName, string tableName, string columnName, bool unique = false) {
        return CreateIndex(indexName, tableName, [columnName], unique);
    }

    /// <summary>
    /// Creates an index for the specified table and column.
    /// </summary>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnName">Name of the column to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public int CreateIndex(string tableName, string columnName, bool unique = false) {
        return CreateIndex(tableName + "_" + columnName, tableName, columnName, unique);
    }

    /// <summary>
    /// Creates an index for the specified table and columns.
    /// </summary>
    /// <param name="tableName">Name of the database table</param>
    /// <param name="columnNames">An array of column names to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public int CreateIndex(string tableName, string[] columnNames, bool unique = false) {
        return CreateIndex(tableName + "_" + string.Join("_", columnNames), tableName, columnNames, unique);
    }

    /// <summary>
    /// Creates an index for the specified object property.
    /// e.g. CreateIndex&lt;Client&gt;(c => c.Name);
    /// </summary>
    /// <typeparam name="T">Type to reflect to a database table.</typeparam>
    /// <param name="property">Property to index</param>
    /// <param name="unique">Whether the index should be unique</param>
    /// <returns>Zero on success.</returns>
    public int CreateIndex<T>(Expression<Func<T, object>> property, bool unique = false) {
        MemberExpression? memberExpression;
        if (property.Body.NodeType is ExpressionType.Convert) {
            memberExpression = ((UnaryExpression)property.Body).Operand as MemberExpression;
        }
        else {
            memberExpression = property.Body as MemberExpression;
        }
        PropertyInfo propertyInfo = memberExpression?.Member as PropertyInfo
            ?? throw new ArgumentException("The lambda expression 'property' should point to a valid Property");
        string propName = propertyInfo.Name;

        TableMapping map = GetMapping<T>();
        string columnName = map.FindColumnWithPropertyName(propName)!.Name;

        return CreateIndex(map.TableName, columnName, unique);
    }

    public class ColumnInfo {
        [Column("name")]
        public string Name { get; set; } = null!;

        public override string ToString() {
            return Name;
        }
    }

    /// <summary>
    /// Query the built-in sqlite table_info table for a specific tables columns.
    /// </summary>
    /// <returns>The columns contains in the table.</returns>
    /// <param name="tableName">Table name.</param>
    public List<ColumnInfo> GetTableInfo(string tableName) {
        string query = "pragma table_info(\"" + tableName + "\")";
        return Query<ColumnInfo>(query);
    }

    void MigrateTable(TableMapping map, List<ColumnInfo> existingColumns) {
        List<TableMapping.Column> columnsToAdd = [];

        foreach (TableMapping.Column column in map.Columns) {
            bool found = false;
            foreach (ColumnInfo existingColumn in existingColumns) {
                found = string.Equals(column.Name, existingColumn.Name, StringComparison.OrdinalIgnoreCase);
                if (found) {
                    break;
                }
            }
            if (!found) {
                columnsToAdd.Add(column);
            }
        }

        foreach (TableMapping.Column columnToAdd in columnsToAdd) {
            string sql = $"alter table \"{map.TableName}\" add column {Orm.SqlDecl(columnToAdd)}";
            Execute(sql);
        }
    }

    /// <summary>
    /// Creates a new SQLiteCommand. Can be overridden to provide a sub-class.
    /// </summary>
    /// <seealso cref="SQLiteCommand.OnInstanceCreated"/>
    protected virtual SQLiteCommand NewCommand() {
        return new SQLiteCommand(this);
    }

    /// <summary>
    /// Creates a new SQLiteCommand given the command text with arguments. Place a '?'
    /// in the command text for each of the arguments.
    /// </summary>
    /// <param name="commandText">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of '?' in the command text.
    /// </param>
    /// <returns>
    /// A <see cref="SQLiteCommand"/>
    /// </returns>
    public SQLiteCommand CreateCommand(string commandText, params IEnumerable<object?> parameters) {
        if (!_open) {
            throw new SQLiteException(SQLite3.Result.Error, "Cannot create commands from unopened database");
        }

        SQLiteCommand command = NewCommand();
        command.CommandText = commandText;
        foreach (object? parameter in parameters) {
            command.Bind(parameter);
        }
        return command;
    }

    /// <summary>
    /// Creates a new SQLiteCommand given the command text with named arguments. Place a "[@:$]VVV"
    /// in the command text for each of the arguments. VVV represents an alphanumeric identifier.
    /// For example, @name :name and $name can all be used in the query.
    /// </summary>
    /// <param name="commandText">
    /// The fully escaped SQL.
    /// </param>
    /// <param name="parameters">
    /// Arguments to substitute for the occurences of "[@:$]VVV" in the command text.
    /// </param>
    /// <returns>
    /// A <see cref="SQLiteCommand" />
    /// </returns>
    public SQLiteCommand CreateCommand(string commandText, Dictionary<string, object> parameters) {
        if (!_open) {
            throw new SQLiteException(SQLite3.Result.Error, "Cannot create commands from unopened database");
        }

        SQLiteCommand command = NewCommand();
        command.CommandText = commandText;
        foreach (KeyValuePair<string, object> parameter in parameters) {
            command.Bind(parameter.Key, parameter.Value);
        }
        return command;
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
    public int Execute(string query, params IEnumerable<object?> parameters) {
        SQLiteCommand command = CreateCommand(query, parameters);

        if (TimeExecution) {
            _stopwatch ??= new Stopwatch();
            _stopwatch.Reset();
            _stopwatch.Start();
        }

        int rowCount = command.ExecuteNonQuery();

        if (TimeExecution) {
            _stopwatch!.Stop();
            _elapsedMilliseconds += _stopwatch.ElapsedMilliseconds;
            Tracer?.Invoke($"Finished in {_stopwatch.ElapsedMilliseconds} ms ({(_elapsedMilliseconds / 1000.0):0.0} s total)");
        }

        return rowCount;
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
    public T ExecuteScalar<T>(string query, params IEnumerable<object?> parameters) {
        SQLiteCommand command = CreateCommand(query, parameters);

        if (TimeExecution) {
            _stopwatch ??= new Stopwatch();
            _stopwatch.Reset();
            _stopwatch.Start();
        }

        T rowCount = command.ExecuteScalar<T>();

        if (TimeExecution) {
            _stopwatch!.Stop();
            _elapsedMilliseconds += _stopwatch.ElapsedMilliseconds;
            Tracer?.Invoke($"Finished in {_stopwatch.ElapsedMilliseconds} ms ({(_elapsedMilliseconds / 1000.0):0.0} s total)");
        }

        return rowCount;
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
    /// </returns>
    public List<T> Query<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        SQLiteCommand command = CreateCommand(query, parameters);
        return command.ExecuteQuery<T>();
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
    /// An enumerable with one result for the first column of each row returned by the query.
    /// </returns>
    public List<T> QueryScalars<T>(string query, params IEnumerable<object?> parameters) {
        SQLiteCommand command = CreateCommand(query, parameters);
        return command.ExecuteQueryScalars<T>().ToList();
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
    /// The enumerator (retrieved by calling GetEnumerator() on the result of this method)
    /// will call sqlite3_step on each call to MoveNext, so the database
    /// connection must remain open for the lifetime of the enumerator.
    /// </returns>
    public IEnumerable<T> DeferredQuery<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        SQLiteCommand command = CreateCommand(query, parameters);
        return command.ExecuteDeferredQuery<T>();
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
    public List<object> Query(TableMapping map, string query, params IEnumerable<object?> parameters) {
        SQLiteCommand command = CreateCommand(query, parameters);
        return command.ExecuteQuery<object>(map);
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
    /// The enumerator (retrieved by calling GetEnumerator() on the result of this method)
    /// will call sqlite3_step on each call to MoveNext, so the database
    /// connection must remain open for the lifetime of the enumerator.
    /// </returns>
    public IEnumerable<object> DeferredQuery(TableMapping map, string query, params IEnumerable<object?> parameters) {
        SQLiteCommand command = CreateCommand(query, parameters);
        return command.ExecuteDeferredQuery<object>(map);
    }

    /// <summary>
    /// Returns a queryable interface to the table represented by the given type.
    /// </summary>
    /// <returns>
    /// A queryable object that is able to translate Where, OrderBy, and Take
    /// queries into native SQL.
    /// </returns>
    public TableQuery<T> Table<T>() where T : new() {
        return new TableQuery<T>(this);
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="primaryKey">
    /// The primary key.
    /// </param>
    /// <returns>
    /// The object with the given primary key. Throws a not found exception if the object is not found.
    /// </returns>
    public T Get<T>(object primaryKey) where T : new() {
        TableMapping map = GetMapping<T>();
        return Query<T>(map.GetByPrimaryKeySql, primaryKey).First();
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="primaryKey">
    /// The primary key.
    /// </param>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The object with the given primary key. Throws a not found exception if the object is not found.
    /// </returns>
    public object Get(object primaryKey, TableMapping map) {
        return Query(map, map.GetByPrimaryKeySql, primaryKey).First();
    }

    /// <summary>
    /// Attempts to retrieve the first object that matches the predicate from the table
    /// associated with the specified type.
    /// </summary>
    /// <param name="predicate">
    /// A predicate for which object to find.
    /// </param>
    /// <returns>
    /// The object that matches the given predicate. Throws a not found exception if the object is not found.
    /// </returns>
    public T Get<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return Table<T>().Where(predicate).First();
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="primaryKey">
    /// The primary key.
    /// </param>
    /// <returns>
    /// The object with the given primary key or null if the object is not found.
    /// </returns>
    public T? Find<T>(object primaryKey) where T : new() {
        var map = GetMapping<T>();
        return Query<T>(map.GetByPrimaryKeySql, primaryKey).FirstOrDefault();
    }

    /// <summary>
    /// Attempts to retrieve an object with the given primary key from the table
    /// associated with the specified type. Use of this method requires that
    /// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
    /// </summary>
    /// <param name="primaryKey">
    /// The primary key.
    /// </param>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The object with the given primary key or null if the object is not found.
    /// </returns>
    public object? Find(object primaryKey, TableMapping map) {
        return Query(map, map.GetByPrimaryKeySql, primaryKey).FirstOrDefault();
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
    public T? Find<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return Table<T>().Where(predicate).FirstOrDefault();
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
    /// The object that matches the given predicate or null if the object is not found.
    /// </returns>
    public T? FindWithQuery<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return Query<T>(query, parameters).FirstOrDefault();
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
    /// The object that matches the given predicate or null if the object is not found.
    /// </returns>
    public object? FindWithQuery(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return Query(map, query, parameters).FirstOrDefault();
    }

    /// <summary>
    /// Whether <see cref="BeginTransaction"/> has been called and the database is waiting for a <see cref="Commit"/>.
    /// </summary>
    public bool IsInTransaction {
        get => _transactionDepth > 0;
    }

    /// <summary>
    /// Begins a new transaction. Call <see cref="Commit"/> to end the transaction.
    /// </summary>
    /// <example cref="InvalidOperationException">Throws if a transaction has already begun.</example>
    public void BeginTransaction() {
        // The BEGIN command only works if the transaction stack is empty,
        //    or in other words if there are no pending transactions.
        // If the transaction stack is not empty when the BEGIN command is invoked,
        //    then the command fails with an error.
        // Rather than crash with an error, we will just ignore calls to BeginTransaction
        //    that would result in an error.
        if (Interlocked.CompareExchange(ref _transactionDepth, 1, 0) == 0) {
            try {
                Execute("begin transaction");
            }
            catch (SQLiteException ex) {
                // It is recommended that applications respond to the errors listed below
                //    by explicitly issuing a ROLLBACK command.
                // TODO: This rollback failsafe should be localized to all throw sites.

                if (ex.Result is SQLite3.Result.IOError or SQLite3.Result.Full or SQLite3.Result.Busy or SQLite3.Result.NoMem or SQLite3.Result.Interrupt) {
                    RollbackTo(null, true);
                }
                throw;
            }
            catch (Exception) {
                Interlocked.Decrement(ref _transactionDepth);
                throw;
            }
        }
        else {
            // Calling BeginTransaction on an already open transaction is invalid
            throw new InvalidOperationException("Cannot begin a transaction while already in a transaction.");
        }
    }

    /// <summary>
    /// Creates a savepoint in the database at the current point in the transaction timeline.
    /// Begins a new transaction if one is not in progress.
    /// <br/>
    /// Call <see cref="RollbackTo(string)"/> to undo transactions since the returned savepoint.<br/>
    /// Call <see cref="Release"/> to commit transactions after the savepoint returned here.<br/>
    /// Call <see cref="Commit"/> to end the transaction, committing all changes.<br/>
    /// </summary>
    /// <returns>A string naming the savepoint.</returns>
    public string SaveTransactionPoint() {
        int depth = Interlocked.Increment(ref _transactionDepth) - 1;
        string retVal = "S" + _rand.Next(short.MaxValue) + "D" + depth;

        try {
            Execute("savepoint " + retVal);
        }
        catch (SQLiteException ex) {
            // It is recommended that applications respond to the errors listed below
            //    by explicitly issuing a ROLLBACK command.
            // TODO: This rollback failsafe should be localized to all throw sites.

            if (ex.Result is SQLite3.Result.IOError or SQLite3.Result.Full or SQLite3.Result.Busy or SQLite3.Result.NoMem or SQLite3.Result.Interrupt) {
                RollbackTo(null, true);
            }
            throw;
        }
        catch (Exception) {
            Interlocked.Decrement(ref _transactionDepth);
            throw;
        }

        return retVal;
    }

    /// <summary>
    /// Rolls back the transaction that was begun by <see cref="BeginTransaction"/> or <see cref="SaveTransactionPoint"/>.
    /// </summary>
    public void Rollback() {
        RollbackTo(null);
    }
    /// <summary>
    /// Rolls back the savepoint created by <see cref="BeginTransaction"/> or SaveTransactionPoint.
    /// </summary>
    /// <param name="savepoint">The name of the savepoint to roll back to, as returned by <see cref="SaveTransactionPoint"/>.  If savepoint is null or empty, this method is equivalent to a call to <see cref="Rollback"/>.</param>
    public void RollbackTo(string? savepoint) {
        RollbackTo(savepoint, false);
    }
    /// <summary>
    /// Rolls back the transaction that was begun by <see cref="BeginTransaction"/>.
    /// </summary>
    /// <param name="savePoint">The name of the savepoint to roll back to, as returned by <see cref="SaveTransactionPoint"/>.  If savepoint is null or empty, this method is equivalent to a call to <see cref="Rollback"/>.</param>
    /// <param name="noThrow">true to avoid throwing exceptions, false otherwise.</param>
    void RollbackTo(string? savePoint, bool noThrow) {
        // Rolling back without a TO clause rolls backs all transactions
        //    and leaves the transaction stack empty.
        try {
            if (string.IsNullOrEmpty(savePoint)) {
                if (Interlocked.Exchange(ref _transactionDepth, 0) > 0) {
                    Execute("rollback");
                }
            }
            else {
                DoSavePointExecute(savePoint!, "rollback to ");
            }
        }
        catch (SQLiteException) {
            if (!noThrow) {
                throw;
            }
        }
        // No need to rollback if there are no transactions open.
    }
    /// <summary>
    /// Releases a savepoint returned from <see cref="SaveTransactionPoint"/>. Releasing a savepoint
    ///    makes changes since that savepoint permanent if the savepoint began the transaction,
    ///    or otherwise the changes are permanent pending a call to <see cref="Commit"/>.
    ///
    /// The RELEASE command is like a COMMIT for a SAVEPOINT.
    /// </summary>
    /// <param name="savePoint">The name of the savepoint to release.  The string should be the result of a call to <see cref="SaveTransactionPoint"/></param>
    public void Release(string savePoint) {
        try {
            DoSavePointExecute(savePoint, "release ");
        }
        catch (SQLiteException ex) {
            if (ex.Result is SQLite3.Result.Busy) {
                // Force a rollback since most people don't know this function can fail
                // Don't call Rollback() since the _transactionDepth is 0 and it won't try
                // Calling rollback makes our _transactionDepth variable correct.
                // Writes to the database only happen at depth=0, so this failure will only happen then.
                try {
                    Execute("rollback");
                }
                catch {
                    // rollback can fail in all sorts of wonderful version-dependent ways. Let's just hope for the best
                }
            }
            throw;
        }
    }
    private void DoSavePointExecute(string savePoint, string command) {
        // Validate the savepoint
        int firstLen = savePoint.IndexOf('D');
        if (firstLen >= 2 && savePoint.Length > firstLen + 1) {
            if (int.TryParse(savePoint.Substring(firstLen + 1), out int depth)) {
                // TODO: Mild race here, but inescapable without locking almost everywhere.
                if (0 <= depth && depth < _transactionDepth) {
                    Volatile.Write(ref _transactionDepth, depth);
                    Execute(command + savePoint);
                    return;
                }
            }
        }

        throw new ArgumentException("savePoint is not valid, and should be the result of a call to SaveTransactionPoint.", nameof(savePoint));
    }
    /// <summary>
    /// Commits the transaction that was begun by <see cref="BeginTransaction"/>.
    /// </summary>
    public void Commit() {
        if (Interlocked.Exchange(ref _transactionDepth, 0) != 0) {
            try {
                Execute("commit");
            }
            catch {
                // Force a rollback since most people don't know this function can fail
                // Don't call Rollback() since the _transactionDepth is 0 and it won't try
                // Calling rollback makes our _transactionDepth variable correct.
                try {
                    Execute("rollback");
                }
                catch {
                    // rollback can fail in all sorts of wonderful version-dependent ways. Let's just hope for the best
                }
                throw;
            }
        }
        // Do nothing on a commit with no open transaction
    }
    /// <summary>
    /// Executes <paramref name="action"/> within a (possibly nested) transaction by wrapping it in a SAVEPOINT. If an
    /// exception occurs the whole transaction is rolled back, not just the current savepoint. The exception
    /// is rethrown.
    /// </summary>
    /// <param name="action">
    /// The <see cref="Action"/> to perform within a transaction. <paramref name="action"/> can contain any number
    /// of operations on the connection but should never call <see cref="BeginTransaction"/> or
    /// <see cref="Commit"/>.
    /// </param>
    public void RunInTransaction(Action action) {
        try {
            string savePoint = SaveTransactionPoint();
            action();
            Release(savePoint);
        }
        finally {
            Rollback();
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

        TableMapping map = GetMapping(obj.GetType());

        if (map.PrimaryKey is not null && map.PrimaryKey.IsAutoGuid) {
            if (Equals(map.PrimaryKey.GetValue(obj), Guid.Empty)) {
                map.PrimaryKey.SetValue(obj, Guid.NewGuid());
            }
        }

        object?[] values = new object[map.Columns.Length];
        for (int i = 0; i < values.Length; i++) {
            values[i] = map.Columns[i].GetValue(obj);
        }

        string query;
        if (map.Columns.Length == 0) {
            query = $"insert {modifier} into \"{map.TableName}\" default values";
        }
        else {
            string columnsSql = string.Join(",", map.Columns.Select(column => "\"" + column.Name + "\""));
            string valuesSql = string.Join(",", map.Columns.Select(column => "?"));
            query = $"insert {modifier} into \"{map.TableName}\"({columnsSql}) values ({valuesSql})";
        }

        int rowCount = 0;
        try {
            rowCount = Execute(query, values);
        }
        catch (SQLiteException ex) {
            if (ex.Result is SQLite3.Result.Constraint && SQLite3.ExtendedErrCode(Handle!) is SQLite3.ExtendedResult.ConstraintNotNull) {
                throw new NotNullConstraintViolationException(ex, map, obj);
            }
            throw;
        }

        if (map.HasAutoIncrementedPrimaryKey) {
            long id = SQLite3.LastInsertRowid(Handle!);
            map.SetAutoIncrementedPrimaryKey(obj, id);
        }

        if (rowCount > 0) {
            OnTableChanged(map, NotifyTableChangedAction.Update);
        }

        return rowCount;
    }
    /// <summary>
    /// Inserts each object into the table, updating any auto-incremented primary keys.<br/>
    /// The <paramref name="modifier"/> is literal SQL added after <c>INSERT</c> (e.g. <c>OR REPLACE</c>).
    /// </summary>
    /// <returns>The number of rows added.</returns>
    public int InsertAll(IEnumerable objects, string? modifier = null, bool runInTransaction = true) {
        int counter = 0;
        if (runInTransaction) {
            RunInTransaction(() => {
                foreach (object obj in objects) {
                    counter += Insert(obj, modifier);
                }
            });
        }
        else {
            foreach (object obj in objects) {
                counter += Insert(obj, modifier);
            }
        }
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
    public int InsertOrReplaceAll(IEnumerable objects, bool runInTransaction = true) {
        int counter = 0;
        if (runInTransaction) {
            RunInTransaction(() => {
                foreach (object obj in objects) {
                    counter += InsertOrReplace(obj);
                }
            });
        }
        else {
            foreach (object obj in objects) {
                counter += InsertOrReplace(obj);
            }
        }
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
    public int InsertOrIgnoreAll(IEnumerable objects, bool runInTransaction = true) {
        int counter = 0;
        if (runInTransaction) {
            RunInTransaction(() => {
                foreach (object obj in objects) {
                    counter += InsertOrIgnore(obj);
                }
            });
        }
        else {
            foreach (object obj in objects) {
                counter += InsertOrIgnore(obj);
            }
        }
        return counter;
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
    public int Update(object obj) {
        if (obj is null) {
            return 0;
        }
        return Update(obj, obj.GetType());
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
    public int Update(object obj, Type objType) {
        if (obj is null || objType is null) {
            return 0;
        }

        TableMapping map = GetMapping(objType);

        TableMapping.Column primaryKey = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't update in table '{map.TableName}' since it has no primary key");

        IEnumerable<TableMapping.Column> columns = map.Columns.Where(column => column != primaryKey);
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
        string query = $"update \"{map.TableName}\" set {string.Join(",", columns.Select(column => $"\"{column.Name}\" = ? "))} where \"{primaryKey.Name}\" = ?";

        int rowCount = 0;
        try {
            rowCount = Execute(query, parameters);
        }
        catch (SQLiteException ex) {
            if (ex.Result is SQLite3.Result.Constraint && SQLite3.ExtendedErrCode(Handle!) is SQLite3.ExtendedResult.ConstraintNotNull) {
                throw new NotNullConstraintViolationException(ex, map, obj);
            }
            throw;
        }

        if (rowCount > 0) {
            OnTableChanged(map, NotifyTableChangedAction.Update);
        }

        return rowCount;
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
    public int UpdateAll(IEnumerable objects, bool runInTransaction = true) {
        int counter = 0;
        if (runInTransaction) {
            RunInTransaction(() => {
                foreach (object obj in objects) {
                    counter += Update(obj);
                }
            });
        }
        else {
            foreach (object obj in objects) {
                counter += Update(obj);
            }
        }
        return counter;
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
        TableMapping map = GetMapping(objectToDelete.GetType());
        TableMapping.Column primaryKey = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't delete in table '{map.TableName}' since it has no primary key");
        string query = $"delete from \"{map.TableName}\" where \"{primaryKey.Name}\" = ?";
        int count = Execute(query, primaryKey.GetValue(objectToDelete));
        if (count > 0) {
            OnTableChanged(map, NotifyTableChangedAction.Delete);
        }
        return count;
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
    public int Delete<T>(object primaryKey) {
        return Delete(primaryKey, GetMapping<T>());
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
    public int Delete(object primaryKey, TableMapping map) {
        TableMapping.Column primaryKeyColumn = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't delete in table '{map.TableName}' since it has no primary key");
        string query = $"delete from \"{map.TableName}\" where \"{primaryKeyColumn.Name}\" = ?";
        var count = Execute(query, primaryKey);
        if (count > 0)
            OnTableChanged(map, NotifyTableChangedAction.Delete);
        return count;
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
    public int DeleteAll<T>() {
        TableMapping map = GetMapping<T>();
        return DeleteAll(map);
    }

    /// <summary>
    /// Deletes all the objects from the specified table.<br/>
    /// WARNING: To be clear, it deletes ALL the objects from the specified table. Do you really want that?
    /// </summary>
    /// <param name="map">
    /// The TableMapping used to identify the table.
    /// </param>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public int DeleteAll(TableMapping map) {
        string query = $"delete from \"{map.TableName}\"";
        int count = Execute(query);
        if (count > 0)
            OnTableChanged(map, NotifyTableChangedAction.Delete);
        return count;
    }

    /// <summary>
    /// Backup the entire database to the specified path.
    /// </summary>
    /// <param name="destinationDatabasePath">Path to backup file.</param>
    /// <param name="databaseName">The name of the database to backup (usually "main").</param>
    public void Backup(string destinationDatabasePath, string databaseName = "main") {
        // Open the destination
        SQLite3.Result result = SQLite3.Open(destinationDatabasePath, out Sqlite3DatabaseHandle destHandle);
        if (result != SQLite3.Result.OK) {
            throw new SQLiteException(result, "Failed to open destination database");
        }

        // Init the backup
        Sqlite3BackupHandle backupHandle = SQLite3.BackupInit(destHandle, databaseName, Handle!, databaseName);
        if (backupHandle is null) {
            SQLite3.Close(destHandle);
            throw new Exception("Failed to create backup");
        }

        // Perform it
        SQLite3.BackupStep(backupHandle, -1);
        SQLite3.BackupFinish(backupHandle);

        // Check for errors
        result = SQLite3.GetResult(destHandle);
        string msg = "";
        if (result != SQLite3.Result.OK) {
            msg = SQLite3.GetErrmsg(destHandle);
        }

        // Close everything and report errors
        SQLite3.Close(destHandle);
        if (result != SQLite3.Result.OK) {
            throw new SQLiteException(result, msg);
        }
    }

    ~SQLiteConnection() {
        Dispose(false);
    }

    public void Dispose() {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void Close() {
        Dispose(true);
    }

    protected virtual void Dispose(bool disposing) {
        if (_open && Handle is not null) {
            try {
                SQLite3.Result result = SQLite3.Close(Handle);
                if (disposing) {
                    if (result is not SQLite3.Result.OK) {
                        string msg = SQLite3.GetErrmsg(Handle);
                        throw new SQLiteException(result, msg);
                    }
                }
            }
            finally {
                Handle = null;
                _open = false;
            }
        }
    }

    void OnTableChanged(TableMapping table, NotifyTableChangedAction action) {
        TableChanged?.Invoke(this, new NotifyTableChangedEventArgs(table, action));
    }

    public event EventHandler<NotifyTableChangedEventArgs>? TableChanged;
}

public class NotifyTableChangedEventArgs(TableMapping table, NotifyTableChangedAction action) : EventArgs {
    public TableMapping Table { get; } = table;
    public NotifyTableChangedAction Action { get; } = action;
}

public enum NotifyTableChangedAction {
    Insert,
    Update,
    Delete,
}

/// <summary>
/// Represents a parsed connection string.
/// </summary>
public class SQLiteConnectionString {
    public string UniqueKey { get; }
    public string DatabasePath { get; }
    public object? Key { get; }
    public SQLiteOpenFlags OpenFlags { get; }
    public Action<SQLiteConnection>? PreKeyAction { get; }
    public Action<SQLiteConnection>? PostKeyAction { get; }
    public string? VfsName { get; }

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
    public SQLiteConnectionString(string databasePath, object? key = null, Action<SQLiteConnection>? preKeyAction = null, Action<SQLiteConnection>? postKeyAction = null, string? vfsName = null)
        : this(databasePath, SQLiteOpenFlags.Create | SQLiteOpenFlags.ReadWrite, key, preKeyAction, postKeyAction, vfsName) {
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
    public SQLiteConnectionString(string databasePath, SQLiteOpenFlags openFlags = SQLiteOpenFlags.Create | SQLiteOpenFlags.ReadWrite, object? key = null, Action<SQLiteConnection>? preKeyAction = null, Action<SQLiteConnection>? postKeyAction = null, string? vfsName = null) {
        if (key is not null && key is not (string or byte[])) {
            throw new ArgumentException("Encryption key must be string or byte array", nameof(key));
        }

        UniqueKey = $"{databasePath}_{(uint)openFlags:X8}";
        Key = key;
        PreKeyAction = preKeyAction;
        PostKeyAction = postKeyAction;
        OpenFlags = openFlags;
        VfsName = vfsName;

        DatabasePath = databasePath;
    }
}

[AttributeUsage(AttributeTargets.Class)]
public class TableAttribute(string name) : Attribute {
    public string Name { get; set; } = name;

    /// <summary>
    /// Flag whether to create the table without <c>rowid</c> (see <see href="https://sqlite.org/withoutrowid.html"/>).<br/>
    /// The default is <see langword="false"/> so that SQLite adds an implicit <c>rowid</c> to every table created.
    /// </summary>
    public bool WithoutRowId { get; set; }
}

[AttributeUsage(AttributeTargets.Property)]
public class ColumnAttribute(string name) : Attribute {
    public string Name { get; set; } = name;
}

[AttributeUsage(AttributeTargets.Property)]
public class PrimaryKeyAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property)]
public class AutoIncrementAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public class IndexedAttribute : Attribute {
    public string? Name { get; set; }
    public int Order { get; set; }
    public virtual bool Unique { get; set; }

    public IndexedAttribute() {
    }

    public IndexedAttribute(string name, int order) {
        Name = name;
        Order = order;
    }
}

[AttributeUsage(AttributeTargets.Property)]
public class IgnoreAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property)]
public class UniqueAttribute : IndexedAttribute {
    public override bool Unique {
        get => true;
        set => throw new InvalidOperationException();
    }
}

[AttributeUsage(AttributeTargets.Property)]
public class MaxLengthAttribute(int length) : Attribute {
    public int Value { get; } = length;
}

/// <summary>
/// Select the collating sequence to use on a column.
/// "BINARY", "NOCASE", and "RTRIM" are supported.
/// "BINARY" is the default.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class CollationAttribute(string collation) : Attribute {
    public string Value { get; } = collation;
}

[AttributeUsage(AttributeTargets.Property)]
public class NotNullAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Enum)]
public class StoreAsTextAttribute : Attribute {
}

public class TableMapping {
    public Type MappedType { get; }
    public string TableName { get; }
    public bool WithoutRowId { get; }
    public Column[] Columns { get; }
    public Column? PrimaryKey { get; }
    public string GetByPrimaryKeySql { get; }
    public CreateFlags CreateFlags { get; }

    internal MapMethod Method { get; } = MapMethod.ByName;

    private readonly Column? _autoIncrementedPrimaryKey;

    public TableMapping(Type type, CreateFlags createFlags = CreateFlags.None) {
        MappedType = type;
        CreateFlags = createFlags;

        TableAttribute? tableAttribute = type.GetCustomAttribute<TableAttribute>();

        TableName = !string.IsNullOrEmpty(tableAttribute?.Name) ? tableAttribute!.Name : MappedType.Name;
        WithoutRowId = tableAttribute is not null && tableAttribute.WithoutRowId;

        MemberInfo[] members = [.. type.GetProperties(), .. type.GetFields()];
        List<Column> columns = new(members.Length);
        foreach (MemberInfo member in members) {
            bool ignore = member.GetCustomAttribute<IgnoreAttribute>() is not null;
            if (!ignore) {
                columns.Add(new Column(member, createFlags));
            }
        }
        Columns = [.. columns];
        foreach (Column column in Columns) {
            if (column.IsAutoIncrement && column.IsPrimaryKey) {
                _autoIncrementedPrimaryKey = column;
            }
            if (column.IsPrimaryKey) {
                PrimaryKey = column;
            }
        }

        if (PrimaryKey is not null) {
            GetByPrimaryKeySql = $"select * from \"{TableName}\" where \"{PrimaryKey.Name}\" = ?";
        }
        else {
            // People should not be calling Get/Find without a primary key
            GetByPrimaryKeySql = $"select * from \"{TableName}\" limit 1";
        }
    }

    public bool HasAutoIncrementedPrimaryKey => _autoIncrementedPrimaryKey is not null;
    public void SetAutoIncrementedPrimaryKey(object obj, long id) {
        _autoIncrementedPrimaryKey?.SetValue(obj, Convert.ChangeType(id, _autoIncrementedPrimaryKey.ColumnType));
    }

    public Column? FindColumnWithPropertyName(string propertyName) {
        return Columns.FirstOrDefault(column => column.PropertyName == propertyName);
    }
    public Column? FindColumn(string columnName) {
        if (Method is not MapMethod.ByName) {
            throw new InvalidOperationException($"This {nameof(TableMapping)} is not mapped by name, but {Method}.");
        }
        return Columns.FirstOrDefault(column => column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
    }

    public class Column {
        public string Name { get; }
        public PropertyInfo? PropertyInfo => _memberInfo as PropertyInfo;
        public string PropertyName { get => _memberInfo.Name; }
        public Type ColumnType { get; }
        public string Collation { get; }
        public bool IsAutoIncrement { get; }
        public bool IsAutoGuid { get; }
        public bool IsPrimaryKey { get; }
        public IEnumerable<IndexedAttribute> Indices { get; }
        public bool IsNullable { get; }
        public int? MaxStringLength { get; }
        public bool StoreAsText { get; }

        private readonly MemberInfo _memberInfo;

        public Column(MemberInfo member, CreateFlags createFlags = CreateFlags.None) {
            _memberInfo = member;
            Type memberType = GetMemberType(member);

            Name = member.GetCustomAttribute<ColumnAttribute>()?.Name ?? member.Name;

            // If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
            ColumnType = Nullable.GetUnderlyingType(memberType) ?? memberType;
            Collation = Orm.GetCollation(member);

            IsPrimaryKey = Orm.IsPrimaryKey(member)
                || (createFlags.HasFlag(CreateFlags.ImplicitPrimaryKey) && string.Equals(member.Name, Orm.ImplicitPrimaryKeyName, StringComparison.OrdinalIgnoreCase));

            bool isAutoIncrement = Orm.IsAutoIncrement(member) || (IsPrimaryKey && ((createFlags & CreateFlags.AutoIncrementPrimaryKey) == CreateFlags.AutoIncrementPrimaryKey));
            IsAutoGuid = isAutoIncrement && ColumnType == typeof(Guid);
            IsAutoIncrement = isAutoIncrement && !IsAutoGuid;

            Indices = Orm.GetIndices(member);
            if (!Indices.Any() && !IsPrimaryKey && createFlags.HasFlag(CreateFlags.ImplicitIndex) && Name.EndsWith(Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)) {
                Indices = [new IndexedAttribute()];
            }
            IsNullable = !(IsPrimaryKey || Orm.IsMarkedNotNull(member));
            MaxStringLength = Orm.MaxStringLength(member);

            StoreAsText = memberType.GetCustomAttribute<StoreAsTextAttribute>() is not null;
        }

        public void SetValue(object obj, object? value) {
            if (_memberInfo is PropertyInfo propertyInfo) {
                if (value is not null && ColumnType.IsEnum) {
                    propertyInfo.SetValue(obj, Enum.ToObject(ColumnType, value));
                }
                else {
                    propertyInfo.SetValue(obj, value);
                }
            }
            else if (_memberInfo is FieldInfo fieldInfo) {
                if (value is not null && ColumnType.IsEnum) {
                    fieldInfo.SetValue(obj, Enum.ToObject(ColumnType, value));
                }
                else {
                    fieldInfo.SetValue(obj, value);
                }
            }
            else {
                throw new InvalidProgramException("Unreachable condition");
            }
        }
        public object? GetValue(object obj) {
            if (_memberInfo is PropertyInfo propertyInfo) {
                return propertyInfo.GetValue(obj);
            }
            else if (_memberInfo is FieldInfo fieldInfo) {
                return fieldInfo.GetValue(obj);
            }
            else {
                throw new InvalidProgramException("Unreachable condition");
            }
        }
        private static Type GetMemberType(MemberInfo memberInfo) {
            return memberInfo switch {
                PropertyInfo propertyInfo => propertyInfo.PropertyType,
                FieldInfo fieldInfo => fieldInfo.FieldType,
                _ => throw new InvalidProgramException($"{nameof(TableMapping)} only supports properties and fields."),
            };
        }
    }

    internal enum MapMethod {
        ByName,
        ByPosition
    }
}

public static class Orm {
    public const string ImplicitPrimaryKeyName = "Id";
    public const string ImplicitIndexSuffix = "Id";

    public static string SqlDecl(TableMapping.Column column) {
        string decl = $"\"{column.Name}\" {SqlType(column)} ";

        if (column.IsPrimaryKey) {
            decl += "primary key ";
        }
        if (column.IsAutoIncrement) {
            decl += "autoincrement ";
        }
        if (!column.IsNullable) {
            decl += "not null ";
        }
        if (!string.IsNullOrEmpty(column.Collation)) {
            decl += "collate " + column.Collation + " ";
        }

        return decl;
    }
    public static string SqlType(TableMapping.Column column) {
        Type clrType = column.ColumnType;
        if (clrType == typeof(bool) || clrType == typeof(byte) || clrType == typeof(sbyte) || clrType == typeof(short) || clrType == typeof(ushort) || clrType == typeof(int) || clrType == typeof(uint) || clrType == typeof(long) || clrType == typeof(ulong)) {
            return "integer";
        }
        else if (clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal)) {
            return "float";
        }
        else if (clrType == typeof(string) || clrType == typeof(StringBuilder) || clrType == typeof(Uri) || clrType == typeof(UriBuilder)) {
            if (column.MaxStringLength is int maxStringLength) {
                return "varchar(" + maxStringLength + ")";
            }
            return "varchar";
        }
        else if (clrType == typeof(TimeSpan)) {
            return "bigint";
        }
        else if (clrType == typeof(DateTime)) {
            return "bigint";
        }
        else if (clrType == typeof(DateTimeOffset)) {
            return "bigint";
        }
        else if (clrType.IsEnum) {
            return column.StoreAsText ? "varchar" : "integer";
        }
        else if (clrType == typeof(byte[])) {
            return "blob";
        }
        else if (clrType == typeof(Guid)) {
            return "varchar(36)";
        }
        else {
            throw new NotSupportedException("Don't know about " + clrType);
        }
    }
    public static bool IsPrimaryKey(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<PrimaryKeyAttribute>() is not null;
    }
    public static bool IsAutoIncrement(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<AutoIncrementAttribute>() is not null;
    }
    public static string GetCollation(MemberInfo memberInfo) {
		return memberInfo.GetCustomAttribute<CollationAttribute>()?.Value ?? "";
    }

    public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttributes<IndexedAttribute>();
    }
    public static int? MaxStringLength(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<MaxLengthAttribute>()?.Value;
    }

    public static bool IsMarkedNotNull(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<NotNullAttribute>() is not null;
    }
}

public partial class SQLiteCommand(SQLiteConnection conn) {
    private readonly SQLiteConnection _conn = conn;
    private readonly List<Binding> _bindings = [];

    public string CommandText { get; set; } = "";

    public int ExecuteNonQuery() {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing: " + this);
        }

        Sqlite3Statement statement = Prepare();
        SQLite3.Result result = SQLite3.Step(statement);
        SQLite3.Finalize(statement);

        if (result is SQLite3.Result.Done) {
            int rowCount = SQLite3.Changes(_conn.Handle!);
            return rowCount;
        }
        else if (result is SQLite3.Result.Error) {
            string msg = SQLite3.GetErrmsg(_conn.Handle!);
            throw new SQLiteException(result, msg);
        }
        else if (result is SQLite3.Result.Constraint) {
            if (SQLite3.ExtendedErrCode(_conn.Handle!) is SQLite3.ExtendedResult.ConstraintNotNull) {
                throw new NotNullConstraintViolationException(result, SQLite3.GetErrmsg(_conn.Handle!));
            }
        }
        throw new SQLiteException(result, SQLite3.GetErrmsg(_conn.Handle!));
    }

    public IEnumerable<T> ExecuteDeferredQuery<T>() {
        return ExecuteDeferredQuery<T>(_conn.GetMapping<T>());
    }
    public List<T> ExecuteQuery<T>() {
        return ExecuteDeferredQuery<T>(_conn.GetMapping<T>()).ToList();
    }
    public List<T> ExecuteQuery<T>(TableMapping map) {
        return ExecuteDeferredQuery<T>(map).ToList();
    }

    /// <summary>
    /// Invoked every time an instance is loaded from the database.
    /// </summary>
    /// <param name='obj'>
    /// The newly created object.
    /// </param>
    /// <remarks>
    /// This can be overridden in combination with the <see cref="SQLiteConnection.NewCommand"/> method to hook into the life-cycle of objects.
    /// </remarks>
    protected virtual void OnInstanceCreated(object obj) {
        // Can be overridden.
    }

    public IEnumerable<T> ExecuteDeferredQuery<T>(TableMapping map) {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing Query: " + this);
        }

        Sqlite3Statement statement = Prepare();
        try {
            TableMapping.Column?[] columns = new TableMapping.Column[SQLite3.ColumnCount(statement)];
            Action<object, Sqlite3Statement, int>?[] fastColumnSetters = new Action<object, Sqlite3Statement, int>?[SQLite3.ColumnCount(statement)];

            if (map.Method is TableMapping.MapMethod.ByPosition) {
                Array.Copy(map.Columns, columns, Math.Min(columns.Length, map.Columns.Length));
            }
            else if (map.Method is TableMapping.MapMethod.ByName) {
                MethodInfo? getSetter = null;
                if (typeof(T) != map.MappedType) {
                    getSetter = typeof(FastColumnSetter)
                        .GetMethod(nameof(FastColumnSetter.GetFastSetter), BindingFlags.NonPublic | BindingFlags.Static)!
                        .MakeGenericMethod(map.MappedType);
                }

                for (int i = 0; i < columns.Length; i++) {
                    string name = SQLite3.ColumnName16(statement, i);
                    columns[i] = map.FindColumn(name);
                    if (columns[i] is TableMapping.Column column) {
                        if (getSetter is not null) {
                            fastColumnSetters[i] = (Action<object, Sqlite3Statement, int>)getSetter.Invoke(null, [column])!;
                        }
                        else {
                            fastColumnSetters[i] = FastColumnSetter.GetFastSetter<T>(column);
                        }
                    }
                }
            }

            while (SQLite3.Step(statement) is SQLite3.Result.Row) {
                object obj = Activator.CreateInstance(map.MappedType)!;
                for (int i = 0; i < columns.Length; i++) {
                    if (columns[i] is not TableMapping.Column column) {
                        continue;
                    }

                    if (fastColumnSetters[i] is Action<object, Sqlite3Statement, int> fastColumnSetter) {
                        fastColumnSetter.Invoke(obj, statement, i);
                    }
                    else {
                        SQLite3.ColType columnType = SQLite3.ColumnType(statement, i);
                        object? value = ReadCol(statement, i, columnType, column.ColumnType);
                        column.SetValue(obj, value);
                    }
                }
                OnInstanceCreated(obj);
                yield return (T)obj;
            }
        }
        finally {
            SQLite3.Finalize(statement);
        }
    }

    public T ExecuteScalar<T>() {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing Query: " + this);
        }

        T Value = default!;

        Sqlite3Statement stmt = Prepare();

        try {
            SQLite3.Result result = SQLite3.Step(stmt);
            if (result is SQLite3.Result.Row) {
                SQLite3.ColType columnType = SQLite3.ColumnType(stmt, 0);
                object? columnValue = ReadCol(stmt, 0, columnType, typeof(T));
                if (columnValue is not null) {
                    Value = (T)columnValue;
                }
            }
            else if (result is SQLite3.Result.Done) {
            }
            else {
                throw new SQLiteException(result, SQLite3.GetErrmsg(_conn.Handle!));
            }
        }
        finally {
            SQLite3.Finalize(stmt);
        }

        return Value;
    }

    public IEnumerable<T> ExecuteQueryScalars<T>() {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing Query: " + this);
        }
        Sqlite3Statement statement = Prepare();
        try {
            if (SQLite3.ColumnCount(statement) < 1) {
                throw new InvalidOperationException("QueryScalars should return at least one column");
            }
            while (SQLite3.Step(statement) == SQLite3.Result.Row) {
                SQLite3.ColType colType = SQLite3.ColumnType(statement, 0);
                object? value = ReadCol(statement, 0, colType, typeof(T));
                if (value is null) {
                    yield return default!;
                }
                else {
                    yield return (T)value;
                }
            }
        }
        finally {
            SQLite3.Finalize(statement);
        }
    }

    public void Bind(string? name, object? value) {
        _bindings.Add(new Binding() {
            Name = name,
            Value = value
        });
    }
    public void Bind(object? value) {
        Bind(null, value);
    }

    public override string ToString() {
        StringBuilder builder = new();
        builder.AppendLine(CommandText);
        int i = 0;
        foreach (Binding binding in _bindings) {
            builder.AppendLine($" {i}: {binding.Value}");
            i++;
        }
        return builder.ToString();
    }

    private Sqlite3Statement Prepare() {
        Sqlite3Statement stmt = SQLite3.Prepare2(_conn.Handle!, CommandText);
        BindAll(stmt);
        return stmt;
    }

    private void BindAll(Sqlite3Statement stmt) {
        int nextIndex = 1;
        foreach (Binding binding in _bindings) {
            if (binding.Name is not null) {
                binding.Index = SQLite3.BindParameterIndex(stmt, binding.Name);
            }
            else {
                binding.Index = nextIndex++;
            }
            BindParameter(stmt, binding.Index, binding.Value);
        }
    }

    internal static void BindParameter(Sqlite3Statement stmt, int index, object? value) {
        if (value is null) {
            SQLite3.BindNull(stmt, index);
        }
        else {
            if (value is int intValue) {
                SQLite3.BindInt(stmt, index, intValue);
            }
            else if (value is string stringValue) {
                SQLite3.BindText(stmt, index, stringValue);
            }
            else if (value is byte or sbyte or ushort or ushort) {
                SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
            }
            else if (value is bool boolValue) {
                SQLite3.BindInt(stmt, index, boolValue ? 1 : 0);
            }
            else if (value is uint or long or ulong) {
                SQLite3.BindInt64(stmt, index, Convert.ToInt64(value));
            }
            else if (value is float or double or decimal) {
                SQLite3.BindDouble(stmt, index, Convert.ToDouble(value));
            }
            else if (value is TimeSpan timeSpanValue) {
                SQLite3.BindInt64(stmt, index, timeSpanValue.Ticks);
            }
            else if (value is DateTime dateTimeValue) {
                SQLite3.BindInt64(stmt, index, dateTimeValue.Ticks);
            }
            else if (value is DateTimeOffset dateTimeOffsetValue) {
                SQLite3.BindInt64(stmt, index, dateTimeOffsetValue.UtcTicks);
            }
            else if (value is byte[] byteArrayValue) {
                SQLite3.BindBlob(stmt, index, byteArrayValue);
            }
            else if (value is Guid or StringBuilder or Uri or UriBuilder) {
                SQLite3.BindText(stmt, index, value.ToString()!);
            }
            else {
                // Now we could possibly get an enum, retrieve cached info
                Type valueType = value.GetType();
                if (valueType.IsEnum) {
                    if (valueType.GetCustomAttribute<StoreAsTextAttribute>() is not null) {
                        SQLite3.BindText(stmt, index, Enum.GetName(valueType, value)!);
                    }
                    else {
                        SQLite3.BindInt(stmt, index, Convert.ToInt32(value));
                    }
                }
                else {
                    throw new NotSupportedException($"Cannot store type: {value.GetType()}");
                }
            }
        }
    }

    private class Binding {
        public string? Name { get; set; }
        public object? Value { get; set; }
        public int Index { get; set; }
    }

    private static object? ReadCol(Sqlite3Statement stmt, int index, SQLite3.ColType type, Type clrType) {
        if (type is SQLite3.ColType.Null) {
            return null;
        }
        else {
            if (Nullable.GetUnderlyingType(clrType) is Type underlyingType) {
                clrType = underlyingType;
            }

            if (clrType == typeof(string)) {
                return SQLite3.ColumnString(stmt, index);
            }
            else if (clrType == typeof(int)) {
                return (int)SQLite3.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(bool)) {
                return SQLite3.ColumnInt(stmt, index) == 1;
            }
            else if (clrType == typeof(double)) {
                return SQLite3.ColumnDouble(stmt, index);
            }
            else if (clrType == typeof(float)) {
                return (float)SQLite3.ColumnDouble(stmt, index);
            }
            else if (clrType == typeof(TimeSpan)) {
                return new TimeSpan(SQLite3.ColumnInt64(stmt, index));
            }
            else if (clrType == typeof(DateTime)) {
                return new DateTime(SQLite3.ColumnInt64(stmt, index));
            }
            else if (clrType == typeof(DateTimeOffset)) {
                return new DateTimeOffset(SQLite3.ColumnInt64(stmt, index), TimeSpan.Zero);
            }
            else if (clrType.IsEnum) {
                if (type is SQLite3.ColType.Text) {
                    string value = SQLite3.ColumnString(stmt, index);
                    return Enum.Parse(clrType, value, true);
                }
                else {
                    return SQLite3.ColumnInt(stmt, index);
                }
            }
            else if (clrType == typeof(long)) {
                return SQLite3.ColumnInt64(stmt, index);
            }
            else if (clrType == typeof(ulong)) {
                return (ulong)SQLite3.ColumnInt64(stmt, index);
            }
            else if (clrType == typeof(uint)) {
                return (uint)SQLite3.ColumnInt64(stmt, index);
            }
            else if (clrType == typeof(decimal)) {
                return (decimal)SQLite3.ColumnDouble(stmt, index);
            }
            else if (clrType == typeof(byte)) {
                return (byte)SQLite3.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(ushort)) {
                return (ushort)SQLite3.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(short)) {
                return (short)SQLite3.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(sbyte)) {
                return (sbyte)SQLite3.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(byte[])) {
                return SQLite3.ColumnByteArray(stmt, index);
            }
            else if (clrType == typeof(Guid)) {
                string text = SQLite3.ColumnString(stmt, index);
                return new Guid(text);
            }
            else if (clrType == typeof(Uri)) {
                string text = SQLite3.ColumnString(stmt, index);
                return new Uri(text);
            }
            else if (clrType == typeof(StringBuilder)) {
                string text = SQLite3.ColumnString(stmt, index);
                return new StringBuilder(text);
            }
            else if (clrType == typeof(UriBuilder)) {
                string text = SQLite3.ColumnString(stmt, index);
                return new UriBuilder(text);
            }
            else {
                throw new NotSupportedException("Don't know how to read " + clrType);
            }
        }
    }
}

internal class FastColumnSetter {
    /// <summary>
    /// Creates a delegate that can be used to quickly set object members from query columns.
    ///
    /// Note that this frontloads the slow reflection-based type checking for columns to only happen once at the beginning of a query,
    /// and then afterwards each row of the query can invoke the delegate returned by this function to get much better performance (up to 10x speed boost, depending on query size and platform).
    /// </summary>
    /// <typeparam name="T">The type of the destination object that the query will read into</typeparam>
    /// <param name="conn">The active connection.  Note that this is primarily needed in order to read preferences regarding how certain data types (such as TimeSpan / DateTime) should be encoded in the database.</param>
    /// <param name="column">The table mapping used to map the statement column to a member of the destination object type</param>
    /// <returns>
    /// A delegate for fast-setting of object members from statement columns.
    ///
    /// If no fast setter is available for the requested column (enums in particular cause headache), then this function returns null.
    /// </returns>
    internal static Action<object, Sqlite3Statement, int>? GetFastSetter<T>(TableMapping.Column column) {
        Type clrType = column.PropertyInfo!.PropertyType;

        if (Nullable.GetUnderlyingType(clrType) is Type underlyingType) {
            clrType = underlyingType;
        }

        if (clrType == typeof(string)) {
            return CreateTypedSetterDelegate<T, string>(column, (stmt, index) => {
                return SQLite3.ColumnString(stmt, index);
            });
        }
        else if (clrType == typeof(int)) {
            return CreateNullableTypedSetterDelegate<T, int>(column, (stmt, index) => {
                return SQLite3.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(bool)) {
            return CreateNullableTypedSetterDelegate<T, bool>(column, (stmt, index) => {
                return SQLite3.ColumnInt(stmt, index) == 1;
            });
        }
        else if (clrType == typeof(double)) {
            return CreateNullableTypedSetterDelegate<T, double>(column, (stmt, index) => {
                return SQLite3.ColumnDouble(stmt, index);
            });
        }
        else if (clrType == typeof(float)) {
            return CreateNullableTypedSetterDelegate<T, float>(column, (stmt, index) => {
                return (float)SQLite3.ColumnDouble(stmt, index);
            });
        }
        else if (clrType == typeof(TimeSpan)) {
            return CreateNullableTypedSetterDelegate<T, TimeSpan>(column, (stmt, index) => {
                return new TimeSpan(SQLite3.ColumnInt64(stmt, index));
            });
        }
        else if (clrType == typeof(DateTime)) {
            return CreateNullableTypedSetterDelegate<T, DateTime>(column, (stmt, index) => {
                return new DateTime(SQLite3.ColumnInt64(stmt, index));
            });
        }
        else if (clrType == typeof(DateTimeOffset)) {
            return CreateNullableTypedSetterDelegate<T, DateTimeOffset>(column, (stmt, index) => {
                return new DateTimeOffset(SQLite3.ColumnInt64(stmt, index), TimeSpan.Zero);
            });
        }
        else if (clrType.IsEnum) {
            // NOTE: Not sure of a good way (if any?) to do a strongly-typed fast setter like this for enumerated types -- for now, return null and column sets will revert back to the safe (but slow) Reflection-based method of column prop.Set()
        }
        else if (clrType == typeof(long)) {
            return CreateNullableTypedSetterDelegate<T, long>(column, (stmt, index) => {
                return SQLite3.ColumnInt64(stmt, index);
            });
        }
        else if (clrType == typeof(ulong)) {
            return CreateNullableTypedSetterDelegate<T, ulong>(column, (stmt, index) => {
                return (ulong)SQLite3.ColumnInt64(stmt, index);
            });
        }
        else if (clrType == typeof(uint)) {
            return CreateNullableTypedSetterDelegate<T, uint>(column, (stmt, index) => {
                return (uint)SQLite3.ColumnInt64(stmt, index);
            });
        }
        else if (clrType == typeof(decimal)) {
            return CreateNullableTypedSetterDelegate<T, decimal>(column, (stmt, index) => {
                return (decimal)SQLite3.ColumnDouble(stmt, index);
            });
        }
        else if (clrType == typeof(byte)) {
            return CreateNullableTypedSetterDelegate<T, byte>(column, (stmt, index) => {
                return (byte)SQLite3.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(ushort)) {
            return CreateNullableTypedSetterDelegate<T, ushort>(column, (stmt, index) => {
                return (ushort)SQLite3.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(short)) {
            return CreateNullableTypedSetterDelegate<T, short>(column, (stmt, index) => {
                return (short)SQLite3.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(sbyte)) {
            return CreateNullableTypedSetterDelegate<T, sbyte>(column, (stmt, index) => {
                return (sbyte)SQLite3.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(byte[])) {
            return CreateTypedSetterDelegate<T, byte[]>(column, (stmt, index) => {
                return SQLite3.ColumnByteArray(stmt, index);
            });
        }
        else if (clrType == typeof(Guid)) {
            return CreateNullableTypedSetterDelegate<T, Guid>(column, (stmt, index) => {
                string text = SQLite3.ColumnString(stmt, index);
                return new Guid(text);
            });
        }
        else if (clrType == typeof(StringBuilder)) {
            return CreateTypedSetterDelegate<T, StringBuilder>(column, (stmt, index) => {
                string text = SQLite3.ColumnString(stmt, index);
                return new StringBuilder(text);
            });
        }
        else if (clrType == typeof(Uri)) {
            return CreateTypedSetterDelegate<T, Uri>(column, (stmt, index) => {
                string text = SQLite3.ColumnString(stmt, index);
                return new Uri(text);
            });
        }
        else if (clrType == typeof(UriBuilder)) {
            return CreateTypedSetterDelegate<T, UriBuilder>(column, (stmt, index) => {
                string text = SQLite3.ColumnString(stmt, index);
                return new UriBuilder(text);
            });
        }
        else {
            // NOTE: Will fall back to the slow setter method in the event that we are unable to create a fast setter delegate for a particular column type
        }
        return null;
    }

    /// <summary>
    /// This creates a strongly typed delegate that will permit fast setting of column values given a Sqlite3Statement and a column index.
    ///
    /// Note that this is identical to CreateTypedSetterDelegate(), but has an extra check to see if it should create a nullable version of the delegate.
    /// </summary>
    /// <typeparam name="ObjectType">The type of the object whose member column is being set</typeparam>
    /// <typeparam name="ColumnMemberType">The CLR type of the member in the object which corresponds to the given SQLite columnn</typeparam>
    /// <param name="column">The column mapping that identifies the target member of the destination object</param>
    /// <param name="getColumnValue">A lambda that can be used to retrieve the column value at query-time</param>
    /// <returns>A strongly-typed delegate</returns>
    private static Action<object, Sqlite3Statement, int> CreateNullableTypedSetterDelegate<ObjectType, ColumnMemberType>(TableMapping.Column column, Func<Sqlite3Statement, int, ColumnMemberType> getColumnValue) where ColumnMemberType : struct {
        Type clrType = column.PropertyInfo!.PropertyType;
        bool isNullable = false;

        if (Nullable.GetUnderlyingType(clrType) is not null) {
            isNullable = true;
        }

        if (isNullable) {
            Action<ObjectType, ColumnMemberType?> setProperty = (Action<ObjectType, ColumnMemberType?>)Delegate.CreateDelegate(
                typeof(Action<ObjectType, ColumnMemberType?>),
                null,
                column.PropertyInfo.GetSetMethod()!
            );

            return (obj, stmt, i) => {
                SQLite3.ColType colType = SQLite3.ColumnType(stmt, i);
                if (colType is not SQLite3.ColType.Null) {
                    setProperty.Invoke((ObjectType)obj, getColumnValue.Invoke(stmt, i));
                }
            };
        }

        return CreateTypedSetterDelegate<ObjectType, ColumnMemberType>(column, getColumnValue);
    }

    /// <summary>
    /// This creates a strongly typed delegate that will permit fast setting of column values given a Sqlite3Statement and a column index.
    /// </summary>
    /// <typeparam name="ObjectType">The type of the object whose member column is being set</typeparam>
    /// <typeparam name="ColumnMemberType">The CLR type of the member in the object which corresponds to the given SQLite columnn</typeparam>
    /// <param name="column">The column mapping that identifies the target member of the destination object</param>
    /// <param name="getColumnValue">A lambda that can be used to retrieve the column value at query-time</param>
    /// <returns>A strongly-typed delegate</returns>
    private static Action<object, Sqlite3Statement, int> CreateTypedSetterDelegate<ObjectType, ColumnMemberType>(TableMapping.Column column, Func<Sqlite3Statement, int, ColumnMemberType> getColumnValue) {
        Action<ObjectType, ColumnMemberType> setProperty = (Action<ObjectType, ColumnMemberType>)Delegate.CreateDelegate(
            typeof(Action<ObjectType, ColumnMemberType>),
            null,
            column.PropertyInfo!.GetSetMethod()!
        );

        return (obj, stmt, i) => {
            SQLite3.ColType colType = SQLite3.ColumnType(stmt, i);
            if (colType != SQLite3.ColType.Null) {
                setProperty.Invoke((ObjectType)obj, getColumnValue.Invoke(stmt, i));
            }
        };
    }
}

public enum CreateTableResult {
    Created,
    Migrated,
}

public class CreateTablesResult {
    public Dictionary<Type, CreateTableResult> Results { get; } = [];
}

public abstract class BaseTableQuery {
    protected class Ordering(string columnName, bool ascending) {
        public string ColumnName { get; } = columnName;
        public bool Ascending { get; } = ascending;
    }
}

public class TableQuery<T> : BaseTableQuery, IEnumerable<T> {
    public SQLiteConnection Connection { get; }
    public TableMapping Table { get; }

    private Expression? _where;
    private List<Ordering>? _orderBys;
    private int? _limit;
    private int? _offset;

    private BaseTableQuery? _joinInner;
    private Expression? _joinInnerKeySelector;
    private BaseTableQuery? _joinOuter;
    private Expression? _joinOuterKeySelector;
    private Expression? _joinSelector;

    private Expression? _selector;

    private bool _deferred;

    private TableQuery(SQLiteConnection connection, TableMapping table) {
        Connection = connection;
        Table = table;
    }
    public TableQuery(SQLiteConnection connection) {
        Connection = connection;
        Table = Connection.GetMapping<T>();
    }

    public TableQuery<U> Clone<U>() {
        TableQuery<U> query = new(Connection, Table) {
            _where = _where,
            _deferred = _deferred,
            _orderBys = _orderBys?.ToList(),
            _limit = _limit,
            _offset = _offset,
            _joinInner = _joinInner,
            _joinInnerKeySelector = _joinInnerKeySelector,
            _joinOuter = _joinOuter,
            _joinOuterKeySelector = _joinOuterKeySelector,
            _joinSelector = _joinSelector,
            _selector = _selector,
        };
        return query;
    }

    /// <summary>
    /// Filters the query based on a predicate.
    /// </summary>
    public TableQuery<T> Where(Expression<Func<T, bool>> predicateExpression) {
        if (predicateExpression.NodeType is ExpressionType.Lambda) {
            LambdaExpression lambda = predicateExpression;
            Expression pred = lambda.Body;
            TableQuery<T> query = Clone<T>();
            query.AddWhere(pred);
            return query;
        }
        else {
            throw new NotSupportedException("Must be a predicate");
        }
    }

    /// <summary>
    /// Delete all the rows that match this query.
    /// </summary>
    public int Delete() {
        return Delete(null);
    }
    /// <summary>
    /// Delete all the rows that match this query and the given predicate.
    /// </summary>
    public int Delete(Expression<Func<T, bool>>? predicateExpression) {
        if (_limit is not null || _offset is not null) {
            throw new InvalidOperationException("Cannot delete with limits or offsets");
        }
        if (_where is null && predicateExpression is null) {
            throw new InvalidOperationException("No condition specified");
        }

        Expression? predicate = _where;
        if (predicateExpression is not null && predicateExpression.NodeType is ExpressionType.Lambda) {
            LambdaExpression lambda = predicateExpression;
            predicate = predicate is not null ? Expression.AndAlso(predicate, lambda.Body) : lambda.Body;
        }

        List<object?> parameters = [];
        string commandText = $"delete from \"{Table.TableName}\" where {CompileExpression(predicate!, parameters).CommandText}";
        SQLiteCommand command = Connection.CreateCommand(commandText, parameters);

        int result = command.ExecuteNonQuery();
        return result;
    }

    /// <summary>
    /// Yields a given number of elements from the query and then skips the remainder.
    /// </summary>
    public TableQuery<T> Take(int n) {
        TableQuery<T> query = Clone<T>();
        query._limit = n;
        return query;
    }
    /// <summary>
    /// Skips a given number of elements from the query and then yields the remainder.
    /// </summary>
    public TableQuery<T> Skip(int n) {
        TableQuery<T> query = Clone<T>();
        query._offset = n;
        return query;
    }

    /// <summary>
    /// Returns the element at a given index.
    /// </summary>
    public T ElementAt(int index) {
        return Skip(index).Take(1).First();
    }

    public TableQuery<T> Deferred() {
        TableQuery<T> query = Clone<T>();
        query._deferred = true;
        return query;
    }

    /// <summary>
    /// Orders the query results according to a key.
    /// </summary>
    public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> expression) {
        return AddOrderBy(expression, true);
    }
    /// <summary>
    /// Orders the query results according to a key.
    /// </summary>
    public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> expression) {
        return AddOrderBy(expression, false);
    }

    private TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpression, bool ascending) {
        LambdaExpression lambdaExpression = orderExpression;

        MemberExpression? memberExpression;
        if (lambdaExpression.Body is UnaryExpression unary && unary.NodeType is ExpressionType.Convert) {
            memberExpression = unary.Operand as MemberExpression;
        }
        else {
            memberExpression = lambdaExpression.Body as MemberExpression;
        }

        if (memberExpression is not null && memberExpression.Expression?.NodeType is ExpressionType.Parameter) {
            TableQuery<T> query = Clone<T>();
            query._orderBys ??= [];
            query._orderBys.Add(new Ordering(Table.FindColumnWithPropertyName(memberExpression.Member.Name)!.Name, ascending));
            return query;
        }
        else {
            throw new NotSupportedException($"Order By does not support: {orderExpression}");
        }
    }

    private void AddWhere(Expression pred) {
        if (_where is null) {
            _where = pred;
        }
        else {
            _where = Expression.AndAlso(_where, pred);
        }
    }

    private SQLiteCommand GenerateCommand(string selectionList) {
        if (_joinInner is not null && _joinOuter is not null) {
            throw new NotSupportedException("Joins are not supported.");
        }

        string commandText = $"select {selectionList} from \"{Table.TableName}\"";
        List<object?> parameters = [];
        if (_where is not null) {
            commandText += $" where {CompileExpression(_where, parameters).CommandText}";
        }
        if ((_orderBys is not null) && (_orderBys.Count > 0)) {
            string orderByString = string.Join(", ", _orderBys.Select(orderBy => $"\"{orderBy.ColumnName}\"" + (orderBy.Ascending ? "" : " desc")));
            commandText += $" order by {orderByString}";
        }
        if (_limit is not null) {
            commandText += $" limit {_limit.Value}";
        }
        if (_offset.HasValue) {
            if (_limit is null) {
                commandText += " limit -1 ";
            }
            commandText += $" offset {_offset.Value}";
        }
        return Connection.CreateCommand(commandText, parameters);
    }

    private class CompileResult {
        public string? CommandText { get; set; }
        public object? Value { get; set; }
    }

    private CompileResult CompileExpression(Expression expression, List<object?> queryParameters) {
        if (expression is null) {
            throw new NotSupportedException("Expression is NULL");
        }
        else if (expression is BinaryExpression binaryExpression) {
            // VB turns 'x=="foo"' into 'CompareString(x,"foo",true/false)==0', so we need to unwrap it
            // http://blogs.msdn.com/b/vbteam/archive/2007/09/18/vb-expression-trees-string-comparisons.aspx
            if (binaryExpression.Left.NodeType is ExpressionType.Call) {
                MethodCallExpression call = (MethodCallExpression)binaryExpression.Left;
                if (call.Method.DeclaringType!.FullName == "Microsoft.VisualBasic.CompilerServices.Operators" && call.Method.Name == "CompareString") {
                    binaryExpression = Expression.MakeBinary(binaryExpression.NodeType, call.Arguments[0], call.Arguments[1]);
                }
            }

            CompileResult leftResult = CompileExpression(binaryExpression.Left, queryParameters);
            CompileResult rightResult = CompileExpression(binaryExpression.Right, queryParameters);

            // If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
            string text;
            if (leftResult.CommandText == "?" && leftResult.Value == null) {
                text = CompileNullBinaryExpression(binaryExpression, rightResult);
            }
            else if (rightResult.CommandText == "?" && rightResult.Value == null) {
                text = CompileNullBinaryExpression(binaryExpression, leftResult);
            }
            else {
                text = "(" + leftResult.CommandText + " " + GetSqlOperator(binaryExpression.NodeType) + " " + rightResult.CommandText + ")";
            }
            return new CompileResult() {
                CommandText = text
            };
        }
        else if (expression.NodeType is ExpressionType.Not) {
            Expression operandExpression = ((UnaryExpression)expression).Operand;
            CompileResult operand = CompileExpression(operandExpression, queryParameters);
            object? value = operand.Value;
            if (value is bool boolValue) {
                value = !boolValue;
            }
            return new CompileResult() {
                CommandText = "NOT(" + operand.CommandText + ")",
                Value = value
            };
        }
        else if (expression.NodeType is ExpressionType.Call) {
            MethodCallExpression call = (MethodCallExpression)expression;
            CompileResult[] callArguments = new CompileResult[call.Arguments.Count];
            CompileResult? callTarget = call.Object is not null ? CompileExpression(call.Object, queryParameters) : null;

            for (int i = 0; i < callArguments.Length; i++) {
                callArguments[i] = CompileExpression(call.Arguments[i], queryParameters);
            }

            string sqlCall = "";

            if (call.Method.Name is "Like" && callArguments.Length == 2) {
                sqlCall = "(" + callArguments[0].CommandText + " like " + callArguments[1].CommandText + ")";
            }
            else if (call.Method.Name is "Contains" && callArguments.Length == 2) {
                sqlCall = "(" + callArguments[1].CommandText + " in " + callArguments[0].CommandText + ")";
            }
            else if (call.Method.Name is "Contains" && callArguments.Length == 1) {
                if (call.Object != null && call.Object.Type == typeof(string)) {
                    sqlCall = "( instr(" + callTarget!.CommandText + "," + callArguments[0].CommandText + ") >0 )";
                }
                else {
                    sqlCall = "(" + callArguments[0].CommandText + " in " + callTarget!.CommandText + ")";
                }
            }
            else if (call.Method.Name is "StartsWith" && callArguments.Length >= 1) {
                StringComparison comparisonType = StringComparison.CurrentCulture;
                if (callArguments.Length == 2) {
                    comparisonType = (StringComparison)callArguments[1].Value!;
                }
                switch (comparisonType) {
                    case StringComparison.Ordinal or StringComparison.CurrentCulture:
                        sqlCall = "( substr(" + callTarget!.CommandText + ", 1, " + callArguments[0].Value!.ToString()!.Length + ") =  " + callArguments[0].CommandText + ")";
                        break;
                    case StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase:
                        sqlCall = "(" + callTarget!.CommandText + " like (" + callArguments[0].CommandText + " || '%'))";
                        break;
                }
            }
            else if (call.Method.Name is "EndsWith" && callArguments.Length >= 1) {
                StringComparison comparisonType = StringComparison.CurrentCulture;
                if (callArguments.Length == 2) {
                    comparisonType = (StringComparison)callArguments[1].Value!;
                }
                switch (comparisonType) {
                    case StringComparison.Ordinal or StringComparison.CurrentCulture:
                        sqlCall = "( substr(" + callTarget!.CommandText + ", length(" + callTarget.CommandText + ") - " + callArguments[0].Value!.ToString()!.Length + "+1, " + callArguments[0].Value!.ToString()!.Length + ") =  " + callArguments[0].CommandText + ")";
                        break;
                    case StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase:
                        sqlCall = "(" + callTarget!.CommandText + " like ('%' || " + callArguments[0].CommandText + "))";
                        break;
                }
            }
            else if (call.Method.Name is "Equals" && callArguments.Length == 1) {
                sqlCall = "(" + callTarget!.CommandText + " = (" + callArguments[0].CommandText + "))";
            }
            else if (call.Method.Name is "ToLower") {
                sqlCall = "(lower(" + callTarget!.CommandText + "))";
            }
            else if (call.Method.Name is "ToUpper") {
                sqlCall = "(upper(" + callTarget!.CommandText + "))";
            }
            else if (call.Method.Name is "Replace" && callArguments.Length == 2) {
                sqlCall = "(replace(" + callTarget!.CommandText + "," + callArguments[0].CommandText + "," + callArguments[1].CommandText + "))";
            }
            else if (call.Method.Name is "IsNullOrEmpty" && callArguments.Length == 1) {
                sqlCall = "(" + callArguments[0].CommandText + " is null or" + callArguments[0].CommandText + " ='' )";
            }
            else {
                sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", callArguments.Select(a => a.CommandText).ToArray()) + ")";
            }

            return new CompileResult() {
                CommandText = sqlCall
            };

        }
        else if (expression.NodeType is ExpressionType.Constant) {
            ConstantExpression constantExpression = (ConstantExpression)expression;
            queryParameters.Add(constantExpression.Value);
            return new CompileResult() {
                CommandText = "?",
                Value = constantExpression.Value
            };
        }
        else if (expression.NodeType is ExpressionType.Convert) {
            UnaryExpression unaryExpression = (UnaryExpression)expression;
            CompileResult valueResult = CompileExpression(unaryExpression.Operand, queryParameters);
            return new CompileResult {
                CommandText = valueResult.CommandText,
                Value = valueResult.Value is not null ? ConvertTo(valueResult.Value, unaryExpression.Type) : null
            };
        }
        else if (expression.NodeType is ExpressionType.MemberAccess) {
            MemberExpression memberExpression = (MemberExpression)expression;

            ParameterExpression? parameterExpression = memberExpression.Expression as ParameterExpression;
            if (parameterExpression is null) {
                if (memberExpression.Expression is UnaryExpression convert && convert.NodeType == ExpressionType.Convert) {
                    parameterExpression = convert.Operand as ParameterExpression;
                }
            }

            if (parameterExpression is not null) {
                // This is a column of our table, output just the column name
                // Need to translate it if that column name is mapped
                string columnName = Table.FindColumnWithPropertyName(memberExpression.Member.Name)!.Name;
                return new CompileResult() {
                    CommandText = $"\"{columnName}\""
                };
            }
            else {
                object? memberTarget = null;
                if (memberExpression.Expression != null) {
                    CompileResult result = CompileExpression(memberExpression.Expression, queryParameters);
                    if (result.Value is null) {
                        throw new NotSupportedException("Member access failed to compile expression");
                    }
                    if (result.CommandText is "?") {
                        queryParameters.RemoveAt(queryParameters.Count - 1);
                    }
                    memberTarget = result.Value;
                }

                // Get the member value
                object? memberValue = memberExpression.Member switch {
                    PropertyInfo propertyInfo => propertyInfo.GetValue(memberTarget),
                    FieldInfo fieldInfo => fieldInfo.GetValue(memberTarget),
                    _ => throw new NotSupportedException($"MemberExpression: {memberExpression.Member.GetType()}")
                };

                // Work special magic for enumerables
                if (memberValue is IEnumerable and not (string or IEnumerable<byte>)) {
                    StringBuilder builder = new();
                    builder.Append('(');
                    string comma = "";
                    foreach (object item in (IEnumerable)memberValue) {
                        queryParameters.Add(item);
                        builder.Append(comma);
                        builder.Append('?');
                        comma = ",";
                    }
                    builder.Append(')');
                    return new CompileResult() {
                        CommandText = builder.ToString(),
                        Value = memberValue
                    };
                }
                else {
                    queryParameters.Add(memberValue);
                    return new CompileResult() {
                        CommandText = "?",
                        Value = memberValue
                    };
                }
            }
        }
        throw new NotSupportedException($"Cannot compile: {expression.NodeType}");
    }

    private static object? ConvertTo(object? obj, Type type) {
        if (Nullable.GetUnderlyingType(type) is Type underlyingType) {
            if (obj is null) {
                return null;
            }
            return Convert.ChangeType(obj, underlyingType);
        }
        else {
            return Convert.ChangeType(obj, type);
        }
    }

    /// <summary>
    /// Compiles a BinaryExpression where one of the parameters is null.
    /// </summary>
    /// <param name="expression">The expression to compile</param>
    /// <param name="parameter">The non-null parameter</param>
    private static string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter) {
        if (expression.NodeType is ExpressionType.Equal) {
            return $"({parameter.CommandText} is ?)";
        }
        else if (expression.NodeType is ExpressionType.NotEqual) {
            return $"({parameter.CommandText} is not ?)";
        }
        else if (expression.NodeType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual) {
            return $"({parameter.CommandText} < ?)"; // always false
        }
        else {
            throw new NotSupportedException($"Cannot compile Null-BinaryExpression with type {expression.NodeType}");
        }
    }
    private static string GetSqlOperator(ExpressionType expressionType) {
        return expressionType switch {
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.And => "&",
            ExpressionType.AndAlso => "and",
            ExpressionType.Or => "|",
            ExpressionType.OrElse => "or",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            _ => throw new NotSupportedException($"Cannot get SQL operator for {expressionType}")
        };
    }

    /// <summary>
    /// Executes SELECT COUNT(*) on the query.
    /// </summary>
    public int Count() {
        return GenerateCommand("count(*)").ExecuteScalar<int>();
    }
    /// <summary>
    /// Executes SELECT COUNT(*) on the query with an additional WHERE clause.
    /// </summary>
    public int Count(Expression<Func<T, bool>> predicate) {
        return Where(predicate).Count();
    }

    public IEnumerator<T> GetEnumerator() {
        return _deferred
            ? GenerateCommand("*").ExecuteDeferredQuery<T>().GetEnumerator()
            : GenerateCommand("*").ExecuteQuery<T>().GetEnumerator();
    }
    IEnumerator IEnumerable.GetEnumerator() {
        return GetEnumerator();
    }

    /// <summary>
    /// Queries the database and returns the results as a List.
    /// </summary>
    public List<T> ToList() {
        return GenerateCommand("*").ExecuteQuery<T>();
    }
    /// <summary>
    /// Queries the database and returns the results as an array.
    /// </summary>
    public T[] ToArray() {
        return [.. GenerateCommand("*").ExecuteQuery<T>()];
    }
    /// <summary>
    /// Returns the first element of this query.
    /// </summary>
    public T First() {
        return Take(1).ToList().First();
    }
    /// <summary>
    /// Returns the first element of this query, or null if no element is found.
    /// </summary>
    public T? FirstOrDefault() {
        return Take(1).ToList().FirstOrDefault();
    }
    /// <summary>
    /// Returns the first element of this query that matches the predicate.
    /// </summary>
    public T First(Expression<Func<T, bool>> predicate) {
        return Where(predicate).First();
    }
    /// <summary>
    /// Returns the first element of this query that matches the predicate, or null if no element is found.
    /// </summary>
    public T? FirstOrDefault(Expression<Func<T, bool>> predicate) {
        return Where(predicate).FirstOrDefault();
    }
}

public static class SQLite3 {
    public enum Result : int {
        OK = 0,
        Error = 1,
        Internal = 2,
        Perm = 3,
        Abort = 4,
        Busy = 5,
        Locked = 6,
        NoMem = 7,
        ReadOnly = 8,
        Interrupt = 9,
        IOError = 10,
        Corrupt = 11,
        NotFound = 12,
        Full = 13,
        CannotOpen = 14,
        LockErr = 15,
        Empty = 16,
        SchemaChngd = 17,
        TooBig = 18,
        Constraint = 19,
        Mismatch = 20,
        Misuse = 21,
        NotImplementedLFS = 22,
        AccessDenied = 23,
        Format = 24,
        Range = 25,
        NonDBFile = 26,
        Notice = 27,
        Warning = 28,
        Row = 100,
        Done = 101
    }

    public enum ExtendedResult : int {
        IOErrorRead = (Result.IOError | (1 << 8)),
        IOErrorShortRead = (Result.IOError | (2 << 8)),
        IOErrorWrite = (Result.IOError | (3 << 8)),
        IOErrorFsync = (Result.IOError | (4 << 8)),
        IOErrorDirFSync = (Result.IOError | (5 << 8)),
        IOErrorTruncate = (Result.IOError | (6 << 8)),
        IOErrorFStat = (Result.IOError | (7 << 8)),
        IOErrorUnlock = (Result.IOError | (8 << 8)),
        IOErrorRdlock = (Result.IOError | (9 << 8)),
        IOErrorDelete = (Result.IOError | (10 << 8)),
        IOErrorBlocked = (Result.IOError | (11 << 8)),
        IOErrorNoMem = (Result.IOError | (12 << 8)),
        IOErrorAccess = (Result.IOError | (13 << 8)),
        IOErrorCheckReservedLock = (Result.IOError | (14 << 8)),
        IOErrorLock = (Result.IOError | (15 << 8)),
        IOErrorClose = (Result.IOError | (16 << 8)),
        IOErrorDirClose = (Result.IOError | (17 << 8)),
        IOErrorSHMOpen = (Result.IOError | (18 << 8)),
        IOErrorSHMSize = (Result.IOError | (19 << 8)),
        IOErrorSHMLock = (Result.IOError | (20 << 8)),
        IOErrorSHMMap = (Result.IOError | (21 << 8)),
        IOErrorSeek = (Result.IOError | (22 << 8)),
        IOErrorDeleteNoEnt = (Result.IOError | (23 << 8)),
        IOErrorMMap = (Result.IOError | (24 << 8)),
        LockedSharedcache = (Result.Locked | (1 << 8)),
        BusyRecovery = (Result.Busy | (1 << 8)),
        CannottOpenNoTempDir = (Result.CannotOpen | (1 << 8)),
        CannotOpenIsDir = (Result.CannotOpen | (2 << 8)),
        CannotOpenFullPath = (Result.CannotOpen | (3 << 8)),
        CorruptVTab = (Result.Corrupt | (1 << 8)),
        ReadonlyRecovery = (Result.ReadOnly | (1 << 8)),
        ReadonlyCannotLock = (Result.ReadOnly | (2 << 8)),
        ReadonlyRollback = (Result.ReadOnly | (3 << 8)),
        AbortRollback = (Result.Abort | (2 << 8)),
        ConstraintCheck = (Result.Constraint | (1 << 8)),
        ConstraintCommitHook = (Result.Constraint | (2 << 8)),
        ConstraintForeignKey = (Result.Constraint | (3 << 8)),
        ConstraintFunction = (Result.Constraint | (4 << 8)),
        ConstraintNotNull = (Result.Constraint | (5 << 8)),
        ConstraintPrimaryKey = (Result.Constraint | (6 << 8)),
        ConstraintTrigger = (Result.Constraint | (7 << 8)),
        ConstraintUnique = (Result.Constraint | (8 << 8)),
        ConstraintVTab = (Result.Constraint | (9 << 8)),
        NoticeRecoverWAL = (Result.Notice | (1 << 8)),
        NoticeRecoverRollback = (Result.Notice | (2 << 8))
    }

    public enum ConfigOption : int {
        SingleThread = 1,
        MultiThread = 2,
        Serialized = 3
    }

    public static Result Open(string filename, out Sqlite3DatabaseHandle db) {
        return (Result)Sqlite3.sqlite3_open(filename, out db);
    }
    public static Result Open(string filename, out Sqlite3DatabaseHandle db, int flags, string? vfsName) {
        return (Result)Sqlite3.sqlite3_open_v2(filename, out db, flags, vfsName);
    }
    public static Result Close(Sqlite3DatabaseHandle db) {
        return (Result)Sqlite3.sqlite3_close_v2(db);
    }
    public static Result BusyTimeout(Sqlite3DatabaseHandle db, int milliseconds) {
        return (Result)Sqlite3.sqlite3_busy_timeout(db, milliseconds);
    }
    public static int Changes(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_changes(db);
    }
    public static Sqlite3Statement Prepare2(Sqlite3DatabaseHandle db, string query) {
        int result = Sqlite3.sqlite3_prepare_v2(db, query, out Sqlite3Statement? stmt);
        if (result != 0) {
            throw new SQLiteException((Result)result, GetErrmsg(db));
        }
        return stmt;
    }
    public static Result Step(Sqlite3Statement stmt) {
        return (Result)Sqlite3.sqlite3_step(stmt);
    }
    public static Result Reset(Sqlite3Statement stmt) {
        return (Result)Sqlite3.sqlite3_reset(stmt);
    }
    public static Result Finalize(Sqlite3Statement stmt) {
        return (Result)Sqlite3.sqlite3_finalize(stmt);
    }
    public static long LastInsertRowid(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_last_insert_rowid(db);
    }
    public static string GetErrmsg(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_errmsg(db).utf8_to_string();
    }
    public static int BindParameterIndex(Sqlite3Statement stmt, string name) {
        return Sqlite3.sqlite3_bind_parameter_index(stmt, name);
    }
    public static int BindNull(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_bind_null(stmt, index);
    }
    public static int BindInt(Sqlite3Statement stmt, int index, int val) {
        return Sqlite3.sqlite3_bind_int(stmt, index, val);
    }
    public static int BindInt64(Sqlite3Statement stmt, int index, long val) {
        return Sqlite3.sqlite3_bind_int64(stmt, index, val);
    }
    public static int BindDouble(Sqlite3Statement stmt, int index, double val) {
        return Sqlite3.sqlite3_bind_double(stmt, index, val);
    }
    public static int BindText(Sqlite3Statement stmt, int index, string val) {
        return Sqlite3.sqlite3_bind_text(stmt, index, val);
    }
    public static int BindBlob(Sqlite3Statement stmt, int index, byte[] val) {
        return Sqlite3.sqlite3_bind_blob(stmt, index, val);
    }
    public static int ColumnCount(Sqlite3Statement stmt) {
        return Sqlite3.sqlite3_column_count(stmt);
    }
    public static string ColumnName(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_name(stmt, index).utf8_to_string();
    }
    public static string ColumnName16(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_name(stmt, index).utf8_to_string();
    }
    public static ColType ColumnType(Sqlite3Statement stmt, int index) {
        return (ColType)Sqlite3.sqlite3_column_type(stmt, index);
    }
    public static int ColumnInt(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_int(stmt, index);
    }
    public static long ColumnInt64(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_int64(stmt, index);
    }
    public static double ColumnDouble(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_double(stmt, index);
    }
    public static string ColumnText(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_text(stmt, index).utf8_to_string();
    }
    public static string ColumnText16(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_text(stmt, index).utf8_to_string();
    }
    public static byte[] ColumnBlob(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_blob(stmt, index).ToArray();
    }
    public static int ColumnBytes(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_bytes(stmt, index);
    }
    public static string ColumnString(Sqlite3Statement stmt, int index) {
        return Sqlite3.sqlite3_column_text(stmt, index).utf8_to_string();
    }
    public static byte[] ColumnByteArray(Sqlite3Statement stmt, int index) {
        int length = ColumnBytes(stmt, index);
        if (length > 0) {
            return ColumnBlob(stmt, index);
        }
        return [];
    }
    public static Result EnableLoadExtension(Sqlite3DatabaseHandle db, int onoff) {
        return (Result)Sqlite3.sqlite3_enable_load_extension(db, onoff);
    }
    public static int LibVersionNumber() {
        return Sqlite3.sqlite3_libversion_number();
    }
    public static Result GetResult(Sqlite3DatabaseHandle db) {
        return (Result)Sqlite3.sqlite3_errcode(db);
    }
    public static ExtendedResult ExtendedErrCode(Sqlite3DatabaseHandle db) {
        return (ExtendedResult)Sqlite3.sqlite3_extended_errcode(db);
    }
    public static Sqlite3BackupHandle BackupInit(Sqlite3DatabaseHandle destDb, string destName, Sqlite3DatabaseHandle sourceDb, string sourceName) {
        return Sqlite3.sqlite3_backup_init(destDb, destName, sourceDb, sourceName);
    }
    public static Result BackupStep(Sqlite3BackupHandle backup, int numPages) {
        return (Result)Sqlite3.sqlite3_backup_step(backup, numPages);
    }
    public static Result BackupFinish(Sqlite3BackupHandle backup) {
        return (Result)Sqlite3.sqlite3_backup_finish(backup);
    }

    public enum ColType : int {
        Integer = 1,
        Float = 2,
        Text = 3,
        Blob = 4,
        Null = 5
    }
}