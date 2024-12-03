using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp;

public class SqliteTable<T> where T : new() {
    public SqliteConnection Connection { get; }
    public string TableName { get; }
    public string? VirtualModule { get; }
    public bool WithoutRowId { get; }
    public SqliteColumn[] Columns { get; }
    public SqliteColumn? PrimaryKey { get; }
    public bool HasAutoIncrementedPrimaryKey { get; }

    internal SqliteTable(SqliteConnection connection, string? tableName = null, string? virtualModule = null) {
        TableAttribute? tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();

        Connection = connection;
        TableName = tableName ?? tableAttribute?.Name ?? typeof(T).Name;
        VirtualModule = virtualModule;
        WithoutRowId = tableAttribute?.WithoutRowId ?? false;

        (Columns, PrimaryKey) = GetColumnsFromMembers();
        CreateOrMigrateTable();
        CreateIndexes();
    }

    public string GetByPrimaryKeySql {
        get {
            if (PrimaryKey is null) {
                throw new InvalidOperationException("Cannot get by primary key because table has no primary key");
            }
            return $"select * from {TableName.SqlQuote()} where {PrimaryKey.Name.SqlQuote()} = ?";
        }
    }

    /// <summary>
    /// Executes "drop table if not exists" on the database.
    /// </summary>
    /// <remarks>
    /// This is non-recoverable.
    /// </remarks>
    public void DeleteTable() {
        string query = $"drop table if exists {TableName.SqlQuote()}";
        Connection.Execute(query);
    }

    /// <summary>
    /// Creates and executes a <see cref="SqliteCommand"/> query.<br/>
    /// Use this method to retrieve rows.
    /// </summary>
    /// <returns>
    /// The rows returned by the query.
    /// </returns>
    /// <remarks>
    /// The enumerator calls <c>sqlite3_step</c> on each call to MoveNext, so the database connection must remain open for the lifetime of the enumerator.
    /// </remarks>
    public IEnumerable<T> Query(string query, params IEnumerable<object?> parameters) {
        return Connection.CreateCommand(query, parameters).ExecuteQuery(this);
    }

    /// <summary>
    /// Retrieves an object with the primary key from the associated table.<br/>
    /// The table must have a designated primary key.
    /// </summary>
    /// <returns>
    /// The object with the primary key, or <see langword="null"/> if the object is not found.
    /// </returns>
    public object? Find(object primaryKey) {
        return Query(GetByPrimaryKeySql, primaryKey).FirstOrDefault();
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
    public object? FindWithQuery(string query, params IEnumerable<object?> parameters) {
        return Query(query, parameters).FirstOrDefault();
    }

    /// <summary>
    /// Creates an index for the specified column(s), facilitating constant lookup times.
    /// </summary>
    public void CreateIndex(string indexName, string tableName, IEnumerable<string> columnNames, bool unique = false) {
        string sql = $"create {(unique ? "unique" : "")} index if not exists {indexName.SqlQuote()} on {tableName.SqlQuote()}({string.Join(", ", columnNames.Select(columnName => columnName.SqlQuote()))})";
        Connection.Execute(sql);
    }
    /// <inheritdoc cref="CreateIndex(string, string, IEnumerable{string}, bool)"/>
    public void CreateIndex(string tableName, IEnumerable<string> columnNames, bool unique = false) {
        CreateIndex($"{tableName}_{string.Join("_", columnNames)}", tableName, columnNames, unique);
    }
    /// <summary>
    /// Creates an index for the specified column(s), facilitating constant lookup times.<br/>
    /// For example:
    /// <code>
    /// CreateIndex&lt;Player&gt;(player => player.Name);
    /// </code>
    /// </summary>
    public void CreateIndex(IEnumerable<Expression<Func<T, object>>> members, bool unique = false) {
        // Convert member names to column names
        List<string> columnNames = [];
        foreach (Expression<Func<T, object>> member in members) {
            MemberExpression? memberExpression = member.Body as MemberExpression;
            // Ignore type cast
            if (member.Body is UnaryExpression body && body.NodeType is ExpressionType.Convert) {
                memberExpression = body.Operand as MemberExpression;
            }

            // Ensure member is valid
            if (memberExpression?.Member is null) {
                throw new ArgumentException("Expression must point to a valid member", nameof(members));
            }

            // Get column name from member name
            string columnName = Columns.First(column => column.ClrMember.Name == memberExpression.Member.Name).Name;
        }

        // Create index for columns
        CreateIndex(TableName, columnNames, unique);
    }
    /// <inheritdoc cref="CreateIndex(IEnumerable{Expression{Func{T, object}}}, bool)"/>
    public void CreateIndex(Expression<Func<T, object>> property, bool unique = false) {
        CreateIndex([property], unique);
    }

    private (SqliteColumn[] Columns, SqliteColumn? PrimaryKey) GetColumnsFromMembers() {
        SqliteColumn? primaryKey = null;

        // Find members through reflection
        MemberInfo[] members = [
            .. typeof(T).GetProperties(),
            .. typeof(T).GetFields(),
        ];

        // Add columns from members
        List<SqliteColumn> columns = [];
        foreach (MemberInfo member in members) {
            // Ensure member is not ignored
            if (member.GetCustomAttribute<IgnoreAttribute>() is not null) {
                continue;
            }
            // Add column from member
            SqliteColumn column = new(Connection, member);
            columns.Add(new SqliteColumn(Connection, member));
            // Set column as primary key
            if (column.IsPrimaryKey) {
                primaryKey = column;
            }
        }
        return ([.. columns], primaryKey);
    }
    private void CreateOrMigrateTable() {
        // Check if the table already exists
        List<ColumnInfo> existingColumns = Connection.GetTableInfo(TableName).ToList();

        // Create new table
        if (Connection.TableExists(TableName)) {
            // Add virtual table modifiers
            string virtualModifier = VirtualModule is not null ? "virtual" : "";
            string usingModifier = VirtualModule is not null ? $"using {VirtualModule.SqlQuote()}" : "";

            // Add column declarations
            string columnDeclarations = string.Join(", ", Columns.Select(Connection.Orm.GetSqlDeclaration));

            // Add without row ID modifier
            string withoutRowIdModifier = WithoutRowId ? "without rowid" : "";

            // Build query
            string query = $"create {virtualModifier} table if not exists {TableName.SqlQuote()} {usingModifier} ({columnDeclarations}) {withoutRowIdModifier}";
            // Execute query
            Connection.Execute(query);
        }
        // Migrate existing table
        else {
            List<SqliteColumn> newColumns = Columns.Where(column
                => !existingColumns.Any(existingColumn => existingColumn.Name.Equals(column.Name, StringComparison.OrdinalIgnoreCase))
            ).ToList();

            foreach (SqliteColumn column in newColumns) {
                string sql = $"alter table {TableName.SqlQuote()} add column {Connection.Orm.GetSqlDeclaration(column)}";
                Connection.Execute(sql);
            }
        }
    }
    private void CreateIndexes() {
        // Get annotated column indexes
        Dictionary<string, IndexInfo> indexes = [];
        foreach (SqliteColumn column in Columns) {
            foreach (IndexedAttribute index in column.Indexes) {
                // Choose name for index
                string indexName = index.Name ?? $"{TableName}_{column.Name}";
                // Find index from another column
                if (!indexes.TryGetValue(indexName, out IndexInfo indexInfo)) {
                    indexInfo = new IndexInfo() {
                        IndexName = indexName,
                        TableName = TableName,
                        Unique = index.Unique,
                        Columns = [],
                    };
                    indexes.Add(indexName, indexInfo);
                }
                // Ensure index attributes are not contradictory
                if (index.Unique != indexInfo.Unique) {
                    throw new Exception("Every column in an index must have the same value for their Unique property.");
                }
                // Add column to index
                indexInfo.Columns.Add(column.Name);
            }
        }
        // Create column indexes
        foreach (IndexInfo index in indexes.Values) {
            CreateIndex(index.IndexName, index.TableName, index.Columns, index.Unique);
        }
    }
}

file record struct IndexInfo {
    public string IndexName;
    public string TableName;
    public bool Unique;
    public List<string> Columns;
}