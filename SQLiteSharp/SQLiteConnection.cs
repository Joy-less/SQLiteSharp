using System.Collections;
using System.Diagnostics;
using System.Reflection;
using System.Linq.Expressions;

namespace SQLiteSharp;

/// <summary>
/// An open connection to a SQLite database.
/// </summary>
public partial class SQLiteConnection : IDisposable {
    private readonly static Dictionary<string, TableMapping> _mappings = [];

    private bool _open;
    private TimeSpan _busyTimeout;
    private Stopwatch? _stopwatch;
    private long _elapsedMilliseconds = 0;

    private int _transactionDepth = 0;
    private readonly Random _rand = new();

    public Sqlite3DatabaseHandle? Handle { get; private set; }

    public event EventHandler<NotifyTableChangedEventArgs>? OnTableChanged;

    /// <summary>
    /// The database path used by this connection.
    /// </summary>
    public string DatabasePath { get; }
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
    public SQLiteConnection(string databasePath, OpenFlags openFlags = OpenFlags.ReadWrite | OpenFlags.Create)
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

        SQLiteInterop.Result result = SQLiteInterop.Open(connectionString.DatabasePath, out Sqlite3DatabaseHandle handle, connectionString.OpenFlags, null);
        Handle = handle;

        if (result is not SQLiteInterop.Result.OK) {
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
    /// The SQLite library version number. <c>3007014</c> refers to <c>v3.7.14</c>.
    /// </summary>
    public int SQLiteVersionNumber => SQLiteInterop.LibVersionNumber();

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
        string quotedKey = Quote(key);
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
        SQLiteInterop.Result result = SQLiteInterop.EnableLoadExtension(Handle!, enabled ? 1 : 0);
        if (result != SQLiteInterop.Result.OK) {
            string errorMessage = SQLiteInterop.GetErrmsg(Handle!);
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
                SQLiteInterop.BusyTimeout(Handle, (int)_busyTimeout.TotalMilliseconds);
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
    /// Creates a table for the given type if it doesn't already exist.<br/>
    /// Indexes are also created for columns with <see cref="IndexedAttribute"/>.
    /// </summary>
    /// <returns>
    /// Whether the table was created or migrated.
    /// </returns>
    public CreateTableResult CreateTable(Type type, CreateFlags createFlags = CreateFlags.None) {
        TableMapping map = GetMapping(type, createFlags);

        // Ensure table has at least one column
        if (map.Columns.Length == 0) {
            throw new Exception($"Cannot create a table without columns (add properties to '{type.FullName}')");
        }

        // Check if the table exists
        CreateTableResult result = CreateTableResult.Created;
        List<ColumnInfo> existingColumns = GetTableInfo(map.TableName);

        // Create new table
        if (existingColumns.Count == 0) {
            // Add virtual table declarations for full-text search
            string virtualModifier = createFlags.HasFlag(CreateFlags.FullTextSearch3 | CreateFlags.FullTextSearch4 | CreateFlags.FullTextSearch5) ? "virtual" : "";
            string usingModifier = createFlags switch {
                CreateFlags.FullTextSearch5 => "using fts5",
                CreateFlags.FullTextSearch4 => "using fts4",
                CreateFlags.FullTextSearch3 => "using fts3",
                _ => ""
            };

            // Add column declarations
            string columnDeclarations = string.Join(", ", map.Columns.Select(Orm.GetSqlDeclaration));

            // Add without row ID modifier
            string withoutRowIdModifier = map.WithoutRowId ? "without rowid" : "";

            // Build query
            string query = $"create {virtualModifier} table if not exists \"{map.TableName}\" {usingModifier} ({columnDeclarations}) {withoutRowIdModifier}";
            // Execute query
            Execute(query);
        }
        // Migrate existing table
        else {
            result = CreateTableResult.Migrated;
            MigrateTable(map, existingColumns);
        }

        // Get indexes to create for columns
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
                    throw new Exception("Every column in an index must have the same value for their Unique property.");
                }

                indexInfo.Columns.Add(new IndexedColumn() {
                    Order = index.Order,
                    ColumnName = column.Name
                });
            }
        }
        // Create indexes for columns
        foreach (string indexName in indexes.Keys) {
            IndexInfo index = indexes[indexName];
            string[] columns = index.Columns.OrderBy(i => i.Order).Select(i => i.ColumnName).ToArray();
            CreateIndex(indexName, index.TableName, columns, index.Unique);
        }

        return result;
    }
    /// <inheritdoc cref="CreateTable(Type, CreateFlags)"/>
    public CreateTableResult CreateTable<T>(CreateFlags createFlags = CreateFlags.None) {
        return CreateTable(typeof(T), createFlags);
    }
    /// <summary>
    /// Creates tables for the given types if they don't already exist.<br/>
    /// Indexes are also created for columns with <see cref="IndexedAttribute"/>.
    /// </summary>
    /// <returns>
    /// Whether the tables were created or migrated.
    /// </returns>
    public Dictionary<Type, CreateTableResult> CreateTables(IEnumerable<Type> types, CreateFlags createFlags = CreateFlags.None) {
        Dictionary<Type, CreateTableResult> results = [];
        foreach (Type type in types) {
            results[type] = CreateTable(type, createFlags);
        }
        return results;
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
        return Query<ColumnInfo>(query).ToList();
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
            string sql = $"alter table \"{map.TableName}\" add column {Orm.GetSqlDeclaration(columnToAdd)}";
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
            throw new SQLiteException(SQLiteInterop.Result.Error, "Cannot create commands from unopened database");
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
            throw new SQLiteException(SQLiteInterop.Result.Error, "Cannot create commands from unopened database");
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
    public IEnumerable<T> Query<T>(string query, params IEnumerable<object?> parameters) where T : new() {
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
        return command.ExecuteQuery<T>();
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
    public IEnumerable<object> Query(TableMapping map, string query, params IEnumerable<object?> parameters) {
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
        return command.ExecuteQuery<object>(map);
    }

    /// <summary>
    /// Creates a queryable interface to the table associated with the given type.
    /// </summary>
    /// <returns>
    /// A queryable object that can perform Where, OrderBy, Count, Take and Skip queries on the table.
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
    /// Retrieves an object with the given primary key from the associated table.<br/>
    /// The table must have a designated primary key.
    /// </summary>
    /// <returns>
    /// The object with the given primary key, or <see langword="null"/> if the object is not found.
    /// </returns>
    public object? Find(object primaryKey, TableMapping map) {
        return Query(map, map.GetByPrimaryKeySql, primaryKey).FirstOrDefault();
    }
    /// <inheritdoc cref="Find(object, TableMapping)"/>
    public T? Find<T>(object primaryKey) where T : new() {
        return Query<T>(GetMapping<T>().GetByPrimaryKeySql, primaryKey).FirstOrDefault();
    }
    /// <summary>
    /// Retrieves the first object matching the predicate from the associated table.
    /// </summary>
    /// <returns>
    /// The first object matching the given predicate, or <see langword="null"/> if no objects match the predicate.
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

                if (ex.Result is SQLiteInterop.Result.IOError or SQLiteInterop.Result.Full or SQLiteInterop.Result.Busy or SQLiteInterop.Result.NoMem or SQLiteInterop.Result.Interrupt) {
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

            if (ex.Result is SQLiteInterop.Result.IOError or SQLiteInterop.Result.Full or SQLiteInterop.Result.Busy or SQLiteInterop.Result.NoMem or SQLiteInterop.Result.Interrupt) {
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
            if (ex.Result is SQLiteInterop.Result.Busy) {
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

        if (map.PrimaryKey is not null && map.PrimaryKey.AutoGuid) {
            if (Equals(map.PrimaryKey.GetValue(obj), Guid.Empty)) {
                map.PrimaryKey.SetValue(obj, Guid.NewGuid());
            }
        }

        TableMapping.Column[] columns = map.Columns;

        // Don't insert auto-incremented columns (unless "OR REPLACE"/"OR IGNORE")
        if (string.IsNullOrEmpty(modifier)) {
            columns = [.. columns.Where(column => !column.AutoIncrement)];
        }

        object?[] values = new object[columns.Length];
        for (int i = 0; i < values.Length; i++) {
            values[i] = columns[i].GetValue(obj);
        }

        string query;
        if (columns.Length == 0) {
            query = $"insert {modifier} into \"{map.TableName}\" default values";
        }
        else {
            string columnsSql = string.Join(",", columns.Select(column => "\"" + column.Name + "\""));
            string valuesSql = string.Join(",", columns.Select(column => "?"));
            query = $"insert {modifier} into \"{map.TableName}\"({columnsSql}) values ({valuesSql})";
        }

        int rowCount = 0;
        try {
            rowCount = Execute(query, values);
        }
        catch (SQLiteException ex) {
            if (ex.Result is SQLiteInterop.Result.Constraint && SQLiteInterop.ExtendedErrCode(Handle!) is SQLiteInterop.ExtendedResult.ConstraintNotNull) {
                throw new NotNullConstraintViolationException(ex, map, obj);
            }
            throw;
        }

        if (map.HasAutoIncrementedPrimaryKey) {
            long id = SQLiteInterop.LastInsertRowid(Handle!);
            map.SetAutoIncrementedPrimaryKey(obj, id);
        }

        if (rowCount > 0) {
            InvokeTableChanged(map, NotifyTableChangedAction.Update);
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
            if (ex.Result is SQLiteInterop.Result.Constraint && SQLiteInterop.ExtendedErrCode(Handle!) is SQLiteInterop.ExtendedResult.ConstraintNotNull) {
                throw new NotNullConstraintViolationException(ex, map, obj);
            }
            throw;
        }

        if (rowCount > 0) {
            InvokeTableChanged(map, NotifyTableChangedAction.Update);
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
            InvokeTableChanged(map, NotifyTableChangedAction.Delete);
        }
        return count;
    }

    /// <summary>
    /// Deletes the object with the specified primary key.
    /// </summary>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public int Delete(object primaryKey, TableMapping map) {
        TableMapping.Column primaryKeyColumn = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't delete in table '{map.TableName}' since it has no primary key");
        string query = $"delete from \"{map.TableName}\" where \"{primaryKeyColumn.Name}\" = ?";
        int count = Execute(query, primaryKey);
        if (count > 0)
            InvokeTableChanged(map, NotifyTableChangedAction.Delete);
        return count;
    }
    /// <inheritdoc cref="Delete(object, TableMapping)"/>
    public int Delete<T>(object primaryKey) {
        return Delete(primaryKey, GetMapping<T>());
    }

    /// <summary>
    /// Deletes all the objects from the specified table.<br/>
    /// Be careful using this.
    /// </summary>
    /// <returns>
    /// The number of objects deleted.
    /// </returns>
    public int DeleteAll(TableMapping map) {
        string query = $"delete from \"{map.TableName}\"";
        int count = Execute(query);
        if (count > 0) {
            InvokeTableChanged(map, NotifyTableChangedAction.Delete);
        }
        return count;
    }
    /// <inheritdoc cref="DeleteAll(TableMapping)"/>
    public int DeleteAll<T>() {
        return DeleteAll(GetMapping<T>());
    }

    /// <summary>
    /// Backup the entire database to the specified path.
    /// </summary>
    /// <param name="destinationDatabasePath">Path to backup file.</param>
    /// <param name="databaseName">The name of the database to backup (usually "main").</param>
    public void Backup(string destinationDatabasePath, string databaseName = "main") {
        // Open the destination
        SQLiteInterop.Result result = SQLiteInterop.Open(destinationDatabasePath, out Sqlite3DatabaseHandle destHandle, OpenFlags.ReadOnly, null);
        if (result is not SQLiteInterop.Result.OK) {
            throw new SQLiteException(result, "Failed to open destination database");
        }

        // Init the backup
        Sqlite3BackupHandle backupHandle = SQLiteInterop.BackupInit(destHandle, databaseName, Handle!, databaseName);
        if (backupHandle is null) {
            SQLiteInterop.Close(destHandle);
            throw new Exception("Failed to create backup");
        }

        // Perform it
        SQLiteInterop.BackupStep(backupHandle, -1);
        SQLiteInterop.BackupFinish(backupHandle);

        // Check for errors
        result = SQLiteInterop.GetResult(destHandle);
        string msg = "";
        if (result != SQLiteInterop.Result.OK) {
            msg = SQLiteInterop.GetErrmsg(destHandle);
        }

        // Close everything and report errors
        SQLiteInterop.Close(destHandle);
        if (result != SQLiteInterop.Result.OK) {
            throw new SQLiteException(result, msg);
        }
    }

    public void Dispose() {
        GC.SuppressFinalize(this);
        if (_open && Handle is not null) {
            try {
                SQLiteInterop.Result result = SQLiteInterop.Close(Handle);
                if (result is not SQLiteInterop.Result.OK) {
                    string msg = SQLiteInterop.GetErrmsg(Handle);
                    throw new SQLiteException(result, msg);
                }
            }
            finally {
                Handle = null;
                _open = false;
            }
        }
    }

    private void InvokeTableChanged(TableMapping table, NotifyTableChangedAction action) {
        OnTableChanged?.Invoke(this, new NotifyTableChangedEventArgs(table, action));
    }
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