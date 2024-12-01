using System.Collections;
using System.Reflection;
using System.Linq.Expressions;
using System.Collections.Concurrent;

namespace SQLiteSharp;

/// <summary>
/// An open connection to a SQLite database.
/// </summary>
public partial class SQLiteConnection : IDisposable {
    public ConcurrentDictionary<Type, TableMapping> Mappings { get; } = [];

    public Sqlite3DatabaseHandle Handle { get; }

    /// <summary>
    /// The options used to open this connection.
    /// </summary>
    public SQLiteConnectionOptions Options { get; }

    /// <summary>
    /// Initializes the raw SQLite Portable Class Library.
    /// </summary>
    static SQLiteConnection() {
        SQLitePCL.Batteries_V2.Init();
    }

    /// <summary>
    /// Creates a new connection to the given SQLite database.
    /// </summary>
    public SQLiteConnection(SQLiteConnectionOptions options) {
        Options = options;

        SQLiteRaw.Result openResult = SQLiteRaw.Open(options.DatabasePath, out Sqlite3DatabaseHandle handle, options.OpenFlags, null);
        Handle = handle;

        if (openResult is not SQLiteRaw.Result.OK) {
            throw new SQLiteException(openResult, $"Could not open database file {Quote(Options.DatabasePath)}: {openResult}");
        }

        BusyTimeout = TimeSpan.FromSeconds(1.0);

        if (options.EncryptionKey is not null) {
            SQLiteRaw.SetKey(Handle, options.EncryptionKey);
        }
    }
    /// <inheritdoc cref="SQLiteConnection(SQLiteConnectionOptions)"/>
    public SQLiteConnection(string databasePath, OpenFlags openFlags = OpenFlags.Recommended)
        : this(new SQLiteConnectionOptions(databasePath, openFlags)) {
    }
    public void Dispose() {
        GC.SuppressFinalize(this);

        if (Handle.IsInvalid) {
            return;
        }

        try {
            SQLiteRaw.Result result = SQLiteRaw.Close(Handle);
            if (result is not SQLiteRaw.Result.OK) {
                string errorMessage = SQLiteRaw.GetErrorMessage(Handle);
                throw new SQLiteException(result, errorMessage);
            }
        }
        finally {
            Handle.Dispose();
        }
    }

    /// <summary>
    /// The SQLite library version number. <c>3007014</c> refers to <c>v3.7.14</c>.
    /// </summary>
    public static int SQLiteVersionNumber => SQLiteRaw.LibVersionNumber();

    /// <summary>
    /// Changes the 256-bit (32-byte) encryption key used to encrypt/decrypt the database.
    /// </summary>
    public void ChangeKey(byte[] key) {
        SQLiteRaw.ChangeKey(Handle, key);
    }

    /// <summary>
    /// Enable or disable extension loading.
    /// </summary>
    public void EnableLoadExtension(bool enabled) {
        SQLiteRaw.Result result = SQLiteRaw.EnableLoadExtension(Handle, enabled ? 1 : 0);
        if (result is not SQLiteRaw.Result.OK) {
            string errorMessage = SQLiteRaw.GetErrorMessage(Handle);
            throw new SQLiteException(result, errorMessage);
        }
    }

    /// <summary>
    /// When an operation can't be completed because a table is locked, the operation will be regularly repeated until <see cref="BusyTimeout"/> has elapsed.
    /// </summary>
    public TimeSpan BusyTimeout {
        get => field;
        set {
            field = value;
            SQLiteRaw.BusyTimeout(Handle, (int)field.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Retrieves the table mapping for the given type, generating it if not found.
    /// </summary>
    /// <returns>
    /// A mapping containing the table column schema and methods to get/set properties of objects.
    /// </returns>
    public TableMapping GetMapping(Type type, CreateFlags createFlags = CreateFlags.None) {
        return Mappings.GetOrAdd(type, type => new TableMapping(type, createFlags));
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
        string query = $"drop table if exists {Quote(map.TableName)}";
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
            // Add virtual table modifiers for full-text search
            string virtualModifier = createFlags.HasFlag(CreateFlags.FullTextSearch3 | CreateFlags.FullTextSearch4 | CreateFlags.FullTextSearch5) ? "virtual" : "";
            string usingModifier = createFlags switch {
                CreateFlags.FullTextSearch5 => "using fts5",
                CreateFlags.FullTextSearch4 => "using fts4",
                CreateFlags.FullTextSearch3 => "using fts3",
                _ => ""
            };

            // Add column declarations
            string columnDeclarations = string.Join(", ", map.Columns.Select(ObjectMapper.GetSqlDeclaration));

            // Add without row ID modifier
            string withoutRowIdModifier = map.WithoutRowId ? "without rowid" : "";

            // Build query
            string query = $"create {virtualModifier} table if not exists {Quote(map.TableName)} {usingModifier} ({columnDeclarations}) {withoutRowIdModifier}";
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
            foreach (IndexedAttribute index in column.Indexes) {
                string indexName = index.Name ?? map.TableName + "_" + column.Name;
                if (!indexes.TryGetValue(indexName, out IndexInfo indexInfo)) {
                    indexInfo = new IndexInfo() {
                        IndexName = indexName,
                        TableName = map.TableName,
                        Unique = index.Unique,
                        Columns = [],
                    };
                    indexes.Add(indexName, indexInfo);
                }

                if (index.Unique != indexInfo.Unique) {
                    throw new Exception("Every column in an index must have the same value for their Unique property.");
                }

                indexInfo.Columns.Add(new IndexedColumn() {
                    Order = index.Order,
                    ColumnName = column.Name,
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
    /// Creates an index for the specified table and columns, enabling constant lookup times for the columns.
    /// </summary>
    public void CreateIndex(string indexName, string tableName, IEnumerable<string> columnNames, bool unique = false) {
        string sql = $"create {(unique ? "unique" : "")} index if not exists {Quote(indexName)} on {Quote(tableName)}({string.Join(", ", columnNames.Select(Quote))})";
        Execute(sql);
    }
    /// <inheritdoc cref="CreateIndex(string, string, IEnumerable{string}, bool)"/>
    public void CreateIndex(string tableName, IEnumerable<string> columnNames, bool unique = false) {
        CreateIndex($"{tableName}_{string.Join("_", columnNames)}", tableName, columnNames, unique);
    }
    /// <summary>
    /// Creates an index for the specified table column.<br/>
    /// For example:
    /// <code>
    /// CreateIndex&lt;Player&gt;(player => player.Name);
    /// </code>
    /// </summary>
    public void CreateIndex<T>(Expression<Func<T, object>> property, bool unique = false) {
        MemberExpression? memberExpression;
        if (property.Body.NodeType is ExpressionType.Convert) {
            memberExpression = ((UnaryExpression)property.Body).Operand as MemberExpression;
        }
        else {
            memberExpression = property.Body as MemberExpression;
        }

        PropertyInfo propertyInfo = memberExpression?.Member as PropertyInfo
            ?? throw new ArgumentException("The lambda expression 'property' should point to a valid Property");

        TableMapping map = GetMapping<T>();
        string columnName = map.FindColumnByMemberName(propertyInfo.Name)!.Name;

        CreateIndex(map.TableName, [columnName], unique);
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
        string query = $"pragma table_info({Quote(tableName)})";
        return Query<ColumnInfo>(query).ToList();
    }

    private void MigrateTable(TableMapping map, List<ColumnInfo> existingColumns) {
        List<TableMapping.Column> columnsToAdd = [];

        foreach (TableMapping.Column column in map.Columns) {
            if (!existingColumns.Any(existingColumn => existingColumn.Name.Equals(column.Name, StringComparison.OrdinalIgnoreCase))) {
                columnsToAdd.Add(column);
            }
        }

        foreach (TableMapping.Column columnToAdd in columnsToAdd) {
            string sql = $"alter table {Quote(map.TableName)} add column {ObjectMapper.GetSqlDeclaration(columnToAdd)}";
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
        if (Handle.IsInvalid) {
            throw new SQLiteException(SQLiteRaw.Result.Error, "Cannot create commands from unopened database");
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
        if (Handle.IsInvalid) {
            throw new SQLiteException(SQLiteRaw.Result.Error, "Cannot create commands from unopened database");
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
        int rowCount = command.ExecuteNonQuery();
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
        T rowCount = command.ExecuteScalar<T>();
        return rowCount;
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
        return CreateCommand(query, parameters).ExecuteQueryScalars<T>().ToList();
    }

    /// <summary>
    /// Executes the query on the database and returns each row of the result using the specified mapping.<br/>
    /// Place a <c>?</c> in the query for each parameter.
    /// </summary>
    /// <returns>
    /// An enumerable for each row returned by the query.
    /// </returns>
    /// <remarks>
    /// The enumerator calls <c>sqlite3_step</c> on each call to MoveNext, so the database connection must remain open for the lifetime of the enumerator.
    /// </remarks>
    public IEnumerable<object> Query(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return CreateCommand(query, parameters).ExecuteQuery<object>(map);
    }
    /// <inheritdoc cref="Query(TableMapping, string, IEnumerable{object?})"/>
    public IEnumerable<T> Query<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return CreateCommand(query, parameters).ExecuteQuery<T>();
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
    /// Retrieves an object with the primary key from the associated table.<br/>
    /// The object must have a designated primary key.
    /// </summary>
    /// <returns>
    /// The object with the primary key. Throws an exception if the object is not found.
    /// </returns>
    public object Get(object primaryKey, TableMapping map) {
        return Query(map, map.GetByPrimaryKeySql, primaryKey).First();
    }
    /// <inheritdoc cref="Get(object, TableMapping)"/>
    public T Get<T>(object primaryKey) where T : new() {
        return Query<T>(GetMapping<T>().GetByPrimaryKeySql, primaryKey).First();
    }
    /// <summary>
    /// Retrieves an object matching the predicate from the associated table.
    /// </summary>
    /// <returns>
    /// The first object matching the predicate. Throws an exception if the object is not found.
    /// </returns>
    public T Get<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return Table<T>().Where(predicate).First();
    }
    /// <summary>
    /// Retrieves an object with the primary key from the associated table.<br/>
    /// The table must have a designated primary key.
    /// </summary>
    /// <returns>
    /// The object with the primary key, or <see langword="null"/> if the object is not found.
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
    /// The first object matching the predicate, or <see langword="null"/> if no objects match the predicate.
    /// </returns>
    public T? Find<T>(Expression<Func<T, bool>> predicate) where T : new() {
        return Table<T>().Where(predicate).FirstOrDefault();
    }
    /// <summary>
    /// Retrieves the first object matching the SQL query from the associated table.
    /// </summary>
    /// <returns>
    /// The first object matching the query, or <see langword="null"/> if no objects match the predicate.
    /// </returns>
    public object? FindWithQuery(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return Query(map, query, parameters).FirstOrDefault();
    }
    /// <inheritdoc cref="FindWithQuery(TableMapping, string, IEnumerable{object?})"/>
    public T? FindWithQuery<T>(string query, params IEnumerable<object?> parameters) where T : new() {
        return Query<T>(query, parameters).FirstOrDefault();
    }

    /// <summary>
    /// Creates a transaction or savepoint for commands to be rolled back or committed.<br/>
    /// Call <see cref="Rollback(string?)"/> to cancel the transaction or <see cref="Commit(string?)"/> to perform the transaction.
    /// </summary>
    public void SavePoint(string? savePointName = null) {
        try {
            // Create savepoint
            if (savePointName is not null) {
                Execute($"savepoint {Quote(savePointName)}");
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
                Execute($"rollback to {Quote(savePointName)}");
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
                Execute($"release {Quote(savePointName)}");
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
            query = $"insert {modifier} into {Quote(map.TableName)} default values";
        }
        else {
            string columnsSql = string.Join(",", columns.Select(column => Quote(column.Name)));
            string valuesSql = string.Join(",", columns.Select(column => "?"));
            query = $"insert {modifier} into {Quote(map.TableName)}({columnsSql}) values ({valuesSql})";
        }

        int rowCount = Execute(query, values);

        if (map.HasAutoIncrementedPrimaryKey) {
            long id = SQLiteRaw.GetLastInsertRowid(Handle);
            map.SetAutoIncrementedPrimaryKey(obj, id);
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

        TableMapping map = GetMapping(obj.GetType());

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
        string query = $"update {Quote(map.TableName)} set {string.Join(",", columns.Select(column => $"{Quote(column.Name)} = ? "))} where \"{primaryKey.Name}\" = ?";

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
    public int Delete(object primaryKey, TableMapping map) {
        TableMapping.Column primaryKeyColumn = map.PrimaryKey
            ?? throw new NotSupportedException($"Can't delete in table '{map.TableName}' since it has no primary key");
        string query = $"delete from {Quote(map.TableName)} where {Quote(primaryKeyColumn.Name)} = ?";
        int rowCount = Execute(query, primaryKey);
        return rowCount;
    }
    /// <inheritdoc cref="Delete(object, TableMapping)"/>
    public int Delete<T>(object primaryKey) {
        return Delete(primaryKey, GetMapping<T>());
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
    public int DeleteAll(TableMapping map) {
        string query = $"delete from {Quote(map.TableName)}";
        int rowCount = Execute(query);
        return rowCount;
    }
    /// <inheritdoc cref="DeleteAll(TableMapping)"/>
    public int DeleteAll<T>() {
        return DeleteAll(GetMapping<T>());
    }

    /// <summary>
    /// Saves a backup of the entire database to the specified path.
    /// </summary>
    public void Backup(string destinationDatabasePath, string databaseName = "main") {
        // Open the destination
        SQLiteRaw.Result result = SQLiteRaw.Open(destinationDatabasePath, out Sqlite3DatabaseHandle destHandle, OpenFlags.Recommended, null);
        if (result is not SQLiteRaw.Result.OK) {
            throw new SQLiteException(result, "Failed to open destination database");
        }

        // Init the backup
        Sqlite3BackupHandle backupHandle = SQLiteRaw.BackupInit(destHandle, databaseName, Handle, databaseName);
        if (backupHandle is null) {
            SQLiteRaw.Close(destHandle);
            throw new Exception("Failed to create backup");
        }

        // Perform it
        SQLiteRaw.BackupStep(backupHandle, -1);
        SQLiteRaw.BackupFinish(backupHandle);

        // Check for errors
        result = SQLiteRaw.GetResult(destHandle);
        string errorMessage = "";
        if (result is not SQLiteRaw.Result.OK) {
            errorMessage = SQLiteRaw.GetErrorMessage(destHandle);
        }

        // Close everything and report errors
        SQLiteRaw.Close(destHandle);
        if (result is not SQLiteRaw.Result.OK) {
            throw new SQLiteException(result, errorMessage);
        }
    }

    /// <inheritdoc cref="EnableLoadExtension(bool)"/>
    public Task EnableLoadExtensionAsync(bool enabled) {
        return Task.Run(() => EnableLoadExtension(enabled));
    }
    /// <inheritdoc cref="CreateTable(Type, CreateFlags)"/>
    public Task<CreateTableResult> CreateTableAsync(Type type, CreateFlags createFlags = CreateFlags.None) {
        return Task.Run(() => CreateTable(type, createFlags));
    }
    /// <inheritdoc cref="CreateTable{T}(CreateFlags)"/>
    public Task<CreateTableResult> CreateTableAsync<T>(CreateFlags createFlags = CreateFlags.None) where T : new() {
        return Task.Run(() => CreateTable<T>(createFlags));
    }
    /// <inheritdoc cref="CreateTables(IEnumerable{Type}, CreateFlags)"/>
    public Task<Dictionary<Type, CreateTableResult>> CreateTablesAsync(IEnumerable<Type> types, CreateFlags createFlags = CreateFlags.None) {
        return Task.Run(() => CreateTables(types, createFlags));
    }
    /// <inheritdoc cref="DropTable{T}()"/>
    public Task<int> DropTableAsync<T>() where T : new() {
        return Task.Run(() => DropTable<T>());
    }
    /// <inheritdoc cref="DropTable(TableMapping)"/>
    public Task<int> DropTableAsync(TableMapping map) {
        return Task.Run(() => DropTable(map));
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
    /// <inheritdoc cref="Delete(object, TableMapping)"/>
    public Task<int> DeleteAsync(object primaryKey, TableMapping map) {
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
    /// <inheritdoc cref="DeleteAll(TableMapping)"/>
    public Task<int> DeleteAllAsync(TableMapping map) {
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
    /// <inheritdoc cref="Get(object, TableMapping)"/>
    public Task<object> GetAsync(object pk, TableMapping map) {
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
    /// <inheritdoc cref="Find(object, TableMapping)"/>
    public Task<object?> FindAsync(object pk, TableMapping map) {
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
    /// <inheritdoc cref="FindWithQuery(TableMapping, string, IEnumerable{object?})"/>
    public Task<object?> FindWithQueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => FindWithQuery(map, query, parameters));
    }
    /// <inheritdoc cref="GetMapping(Type, CreateFlags)"/>
    public Task<TableMapping> GetMappingAsync(Type type, CreateFlags createFlags = CreateFlags.None) {
        return Task.Run(() => GetMapping(type, createFlags));
    }
    /// <inheritdoc cref="GetMappingAsync(CreateFlags)"/>
    public Task<TableMapping> GetMappingAsync<T>(CreateFlags createFlags = CreateFlags.None) where T : new() {
        return Task.Run(() => GetMapping<T>(createFlags));
    }
    /// <inheritdoc cref="GetTableInfo(string)"/>
    public Task<List<ColumnInfo>> GetTableInfoAsync(string tableName) {
        return Task.Run(() => GetTableInfo(tableName));
    }
    /// <inheritdoc cref="Execute(string, IEnumerable{object?})"/>
    public Task<int> ExecuteAsync(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => Execute(query, parameters));
    }
    /// <inheritdoc cref="RunInTransaction(Action)"/>
    public Task RunInTransactionAsync(Action action) {
        return Task.Run(() => RunInTransaction(action));
    }
    /// <inheritdoc cref="Table{T}"/>
    public AsyncTableQuery<T> TableAsync<T>() where T : new() {
        return new AsyncTableQuery<T>(Table<T>());
    }
    /// <inheritdoc cref="ExecuteScalar{T}(string, IEnumerable{object?})"/>
    public Task<T> ExecuteScalarAsync<T>(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => ExecuteScalar<T>(query, parameters));
    }
    /// <inheritdoc cref="Query(TableMapping, string, IEnumerable{object?})"/>
    public Task<IEnumerable<object>> QueryAsync(TableMapping map, string query, params IEnumerable<object?> parameters) {
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