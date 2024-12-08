using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp;

/*public abstract class SqliteTable {

}*/
public class SqliteTable<T> /*: SqliteTable*/ where T : notnull, new() {
    public SqliteConnection Connection { get; }
    public string Name { get; }
    public string? VirtualModule { get; }
    public bool WithoutRowId { get; }
    public SqliteColumn[] Columns { get; }
    public SqliteColumn? PrimaryKey { get; }
    public bool HasAutoIncrementedPrimaryKey { get; }

    internal SqliteTable(SqliteConnection connection, string? name = null, string? virtualModule = null, bool createTable = true) {
        TableAttribute? tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
        WithoutRowIdAttribute? withoutRowIdAttribute = typeof(T).GetCustomAttribute<WithoutRowIdAttribute>();

        Connection = connection;
        Name = name ?? tableAttribute?.Name ?? typeof(T).Name;
        VirtualModule = virtualModule;
        WithoutRowId = withoutRowIdAttribute is not null;

        (Columns, PrimaryKey) = GetColumnsFromMembers();

        if (createTable) {
            CreateOrMigrateTable();
            CreateIndexes();
        }
    }

    /// <summary>
    /// Executes "drop table if not exists" on the database.
    /// </summary>
    /// <remarks>
    /// This is non-recoverable.
    /// </remarks>
    public void DeleteTable() {
        string query = $"drop table if exists {Name.SqlQuote()}";
        Connection.Execute(query);
    }
    /// <inheritdoc cref="DeleteTable()"/>
    public Task DeleteTableAsync() {
        return Task.Run(DeleteTable);
    }

    public long Count(Expression<Func<T, bool>>? predicate = null) {
        SqlBuilder<T> query = Build().Select(SelectType.Count);
        if (predicate is not null) {
            query.Where(predicate);
        }
        string x = query.GetCommand();
        _ = x;
        return query.Get<long>().First();
    }
    public Task<long> CountAsync(Expression<Func<T, bool>>? predicate = null) {
        return Task.Run(() => Count(predicate));
    }

    /// <summary>
    /// Builds a complex query using <see cref="DotNetBrightener.LinQToSqlBuilder"/>.<br/>
    /// The query can be executed using <see cref="ExecuteQuery(SqlBuilder{T})"/>.
    /// </summary>
    public SqlBuilder<T> Build() {
        return new SqlBuilder<T>(this);
    }

    /// <summary>
    /// Creates and executes a <see cref="SqliteCommand"/> query.<br/>
    /// Use this method to retrieve rows.
    /// </summary>
    /// <returns>
    /// The rows returned by the query.
    /// </returns>
    /// <remarks>
    /// The <see cref="SqliteConnection"/> must remain open for the lifetime of the enumerator.
    /// </remarks>
    public IEnumerable<T> ExecuteQuery(string query, params IEnumerable<object?> parameters) {
        return Connection.CreateCommand(query, parameters).ExecuteQuery(this);
    }
    /// <inheritdoc cref="ExecuteQuery(string, IEnumerable{object?})"/>
    public IAsyncEnumerable<T> ExecuteQueryAsync(string query, params IEnumerable<object?> parameters) {
        return ExecuteQuery(query, parameters).ToAsyncEnumerable();
    }

    /// <inheritdoc cref="ExecuteQuery(string, IEnumerable{object?})"/>
    public IEnumerable<T> ExecuteQuery(string query, IDictionary<string, object?> parameters) {
        return Connection.CreateCommand(query, parameters).ExecuteQuery(this);
    }
    /// <inheritdoc cref="ExecuteQuery(string, IEnumerable{object?})"/>
    public IAsyncEnumerable<T> ExecuteQueryAsync(string query, IDictionary<string, object?> parameters) {
        return ExecuteQuery(query, parameters).ToAsyncEnumerable();
    }

    /// <summary>
    /// Retrieves an row with the primary key.<br/>
    /// The table must have a designated primary key.
    /// </summary>
    /// <returns>
    /// The row with the primary key, or <see langword="null"/> if the row is not found.
    /// </returns>
    public T? FindByKey(object primaryKey) {
        // Ensure table has primary key
        if (PrimaryKey is null) {
            throw new NotSupportedException($"Can't find in table '{Name}' since it has no annotated primary key");
        }

        // Build select SQL
        string query = $"select * from {Name.SqlQuote()} where {PrimaryKey.Name.SqlQuote()} = ?";

        // Execute select
        return ExecuteQuery(query, primaryKey).FirstOrDefault();
    }
    /// <inheritdoc cref="FindByKey(object)"/>
    public Task<T?> FindByKeyAsync(object primaryKey) {
        return Task.Run(() => FindByKey(primaryKey));
    }

    /// <summary>
    /// Retrieves the first row matching the predicate.
    /// </summary>
    /// <returns>
    /// The first row matching the predicate, or <see langword="null"/> if no rows match the predicate.
    /// </returns>
    public T? FindOne(Expression<Func<T, bool>> predicate) {
        return default; // TODO
        //return Table<T>().Where(predicate).FirstOrDefault();
    }
    /// <inheritdoc cref="FindOne(Expression{Func{T, bool}})"/>
    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => FindOne(predicate));
    }

    /*/// <summary>
    /// Retrieves the first row matching the SQL query from the associated table.
    /// </summary>
    /// <returns>
    /// The first row matching the query, or <see langword="null"/> if no rows match the query.
    /// </returns>
    public T? FindOneByQuery(string query, params IEnumerable<object?> parameters) {
        return ExecuteQuery(query, parameters).FirstOrDefault();
    }
    /// <inheritdoc cref="FindOneByQuery(string, IEnumerable{object?})"/>
    public Task<T?> FindOneByQueryAsync(string query, params IEnumerable<object?> parameters) {
        return Task.Run(() => FindOneByQuery(query, parameters));
    }*/

    /// <summary>
    /// Inserts the row into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <param name="modifier">
    /// Literal SQL added after <c>INSERT</c>:
    /// <code>
    /// OR REPLACE
    /// OR IGNORE
    /// OR ABORT
    /// OR FAIL
    /// OR ROLLBACK
    /// </code>
    /// </param>
    /// <returns>The number of rows added.</returns>
    public int Insert(T row, string? modifier = null) {
        SqliteColumn[] columns = Columns;
        // Strip auto-incremented columns (unless "OR REPLACE"/"OR IGNORE")
        if (string.IsNullOrEmpty(modifier)) {
            columns = [.. columns.Where(column => !column.IsAutoIncrement)];
        }

        // Get column values for object (row)
        IEnumerable<object?> values = columns.Select(column => column.GetValue(row));

        string query;
        if (columns.Length == 0) {
            query = $"insert {modifier} into {Name.SqlQuote()} default values";
        }
        else {
            string columnsSql = string.Join(",", columns.Select(column => column.Name.SqlQuote()));
            string valuesSql = string.Join(",", columns.Select(column => "?"));
            query = $"insert {modifier} into {Name.SqlQuote()}({columnsSql}) values ({valuesSql})";
        }

        int rowCount = Connection.Execute(query, values);

        if (HasAutoIncrementedPrimaryKey) {
            long rowId = SqliteRaw.GetLastInsertRowId(Connection.Handle);
            PrimaryKey?.SetSqliteValue(row, rowId);
        }

        return rowCount;
    }
    /// <inheritdoc cref="Insert(T, string?)"/>
    public Task<int> InsertAsync(T row, string? orModifier = null) {
        return Task.Run(() => Insert(row, orModifier));
    }

    /// <summary>
    /// Inserts each row into the table, updating any auto-incremented primary keys.<br/>
    /// The <paramref name="modifier"/> is literal SQL added after <c>INSERT</c> (e.g. <c>OR REPLACE</c>).
    /// </summary>
    /// <returns>The number of rows added.</returns>
    public int InsertAll(IEnumerable<T> rows, string? modifier = null) {
        int counter = 0;
        Connection.RunInTransaction(() => {
            foreach (T row in rows) {
                counter += Insert(row, modifier);
            }
        });
        return counter;
    }
    /// <inheritdoc cref="InsertAll(IEnumerable{T}, string?)"/>
    public Task<int> InsertAllAsync(IEnumerable<T> rows, string? modifier = null) {
        return Task.Run(() => InsertAll(rows, modifier));
    }

    /// <summary>
    /// Inserts the row into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the old row is replaced.
    /// </remarks>
    /// <returns>The number of rows added/modified.</returns>
    public int InsertOrReplace(T row) {
        return Insert(row, "OR REPLACE");
    }
    /// <inheritdoc cref="InsertOrReplace(T)"/>
    public Task<int> InsertOrReplaceAsync(T row) {
        return Task.Run(() => InsertOrReplace(row));
    }

    /// <summary>
    /// Inserts each row into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the old row is replaced.
    /// </remarks>
    /// <returns>The number of rows added/modified.</returns>
    public int InsertOrReplaceAll(IEnumerable<T> rows) {
        int counter = 0;
        Connection.RunInTransaction(() => {
            foreach (T row in rows) {
                counter += InsertOrReplace(row);
            }
        });
        return counter;
    }
    /// <inheritdoc cref="InsertOrReplaceAll(IEnumerable{T})"/>
    public Task<int> InsertOrReplaceAllAsync(IEnumerable<T> rows) {
        return Task.Run(() => InsertOrReplaceAll(rows));
    }

    /// <summary>
    /// Inserts the row into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the new row is not inserted.
    /// </remarks>
    /// <returns>The number of rows modified.</returns>
    public int InsertOrIgnore(T row) {
        return Insert(row, "OR IGNORE");
    }
    /// <inheritdoc cref="InsertOrIgnore(T)"/>
    public Task<int> InsertOrIgnoreAsync(T row) {
        return Task.Run(() => InsertOrIgnore(row));
    }

    /// <summary>
    /// Inserts each row into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <remarks>
    /// If a UNIQUE constraint violation occurs, the new row is not inserted.
    /// </remarks>
    /// <returns>The number of rows added/modified.</returns>
    public int InsertOrIgnoreAll(IEnumerable<T> rows) {
        int counter = 0;
        Connection.RunInTransaction(() => {
            foreach (T row in rows) {
                counter += InsertOrIgnore(row);
            }
        });
        return counter;
    }
    /// <inheritdoc cref="InsertOrIgnoreAll(IEnumerable{T})"/>
    public Task<int> InsertOrIgnoreAllAsync(IEnumerable<T> rows) {
        return Task.Run(() => InsertOrIgnoreAll(rows));
    }

    /// <summary>
    /// Updates every column of a table using the specified row except for its primary key.
    /// </summary>
    /// <remarks>
    /// The table must have a designated primary key.
    /// </remarks>
    /// <returns>
    /// The number of rows updated.
    /// </returns>
    public int UpdateOne(T row) {
        // Ensure table has primary key
        if (PrimaryKey is null) {
            throw new NotSupportedException($"Can't update in table '{Name}' since it has no annotated primary key");
        }

        // Get column and values to update
        IEnumerable<SqliteColumn> columns = Columns.Where(column => column != PrimaryKey);
        IEnumerable<object?> values = columns.Select(column => column.GetValue(row));

        // Ensure at least one column will be updated
        if (!columns.Any()) {
            return 0;
        }

        // Build update SQL with parameters
        List<object?> parameters = [.. values, PrimaryKey.GetValue(row)];
        string columnsSql = string.Join(",", columns.Select(column => $"{column.Name.SqlQuote()} = ?"));
        string query = $"update {Name.SqlQuote()} set {columnsSql} where {PrimaryKey.Name.SqlQuote()} = ?";

        // Execute update
        int rowCount = Connection.Execute(query, parameters);
        return rowCount;
    }
    /// <inheritdoc cref="UpdateOne(T)"/>
    public Task<int> UpdateOneAsync(T row) {
        return Task.Run(() => UpdateOne(row));
    }

    /// <summary>
    /// Updates every column of a table using the specified rows except for their primary key.
    /// </summary>
    /// <remarks>
    /// The table must have a designated primary key.
    /// </remarks>
    /// <returns>
    /// The number of rows updated.
    /// </returns>
    public int UpdateAll(IEnumerable<T> rows) {
        int counter = 0;
        Connection.RunInTransaction(() => {
            foreach (T row in rows) {
                counter += UpdateOne(row);
            }
        });
        return counter;
    }
    /// <inheritdoc cref="UpdateAll(IEnumerable{T})"/>
    public Task<int> UpdateAllAsync(IEnumerable<T> rows) {
        return Task.Run(() => UpdateAll(rows));
    }

    /// <summary>
    /// Deletes the row with the specified primary key.
    /// </summary>
    /// <returns>
    /// The number of rows deleted.
    /// </returns>
    public int DeleteByKey(object primaryKey) {
        // Ensure table has primary key
        if (PrimaryKey is null) {
            throw new NotSupportedException($"Can't delete in table '{Name}' since it has no annotated primary key");
        }

        // Build delete SQL
        string query = $"delete from {Name.SqlQuote()} where {PrimaryKey.Name.SqlQuote()} = ?";

        // Execute delete
        int rowCount = Connection.Execute(query, primaryKey);
        return rowCount;
    }
    /// <inheritdoc cref="DeleteByKey(object)"/>
    public Task<int> DeleteByKeyAsync(object primaryKey) {
        return Task.Run(() => DeleteByKey(primaryKey));
    }

    /// <summary>
    /// Deletes the rows with the specified primary key.
    /// </summary>
    /// <returns>
    /// The number of rows deleted.
    /// </returns>
    public int DeleteAllByKey(IEnumerable primaryKeys) {
        int counter = 0;
        Connection.RunInTransaction(() => {
            foreach (object primaryKey in primaryKeys) {
                counter += DeleteByKey(primaryKey);
            }
        });
        return counter;
    }
    /// <inheritdoc cref="DeleteAllByKey(IEnumerable)"/>
    public Task<int> DeleteAllByKeyAsync(IEnumerable primaryKeys) {
        return Task.Run(() => DeleteAllByKey(primaryKeys));
    }

    /// <summary>
    /// Deletes every object from the specified table.
    /// </summary>
    /// <remarks>
    /// This is non-recoverable.
    /// </remarks>
    /// <returns>
    /// The number of rows deleted.
    /// </returns>
    public int DeleteAll() {
        string query = $"delete from {Name.SqlQuote()}";
        int rowCount = Connection.Execute(query);
        return rowCount;
    }
    /// <inheritdoc cref="DeleteAll()"/>
    public Task<int> DeleteAllAsync() {
        return Task.Run(DeleteAll);
    }

    /// <summary>
    /// Creates an index for the specified column(s), facilitating constant lookup times.
    /// </summary>
    public void CreateIndex(string indexName, IEnumerable<string> columnNames, bool unique = false) {
        string sql = $"create {(unique ? "unique" : "")} index if not exists {indexName.SqlQuote()} on {Name.SqlQuote()}({string.Join(", ", columnNames.Select(columnName => columnName.SqlQuote()))})";
        Connection.Execute(sql);
    }
    /// <inheritdoc cref="CreateIndex(string, IEnumerable{string}, bool)"/>
    public Task CreateIndexAsync(string indexName, IEnumerable<string> columnNames, bool unique = false) {
        return Task.Run(() => CreateIndex(indexName, columnNames, unique));
    }

    /// <inheritdoc cref="CreateIndex(string, IEnumerable{string}, bool)"/>
    public void CreateIndex(IEnumerable<string> columnNames, bool unique = false) {
        CreateIndex($"{Name}_{string.Join("_", columnNames)}", columnNames, unique);
    }
    /// <inheritdoc cref="CreateIndex(IEnumerable{string}, bool)"/>
    public Task CreateIndexAsync(IEnumerable<string> columnNames, bool unique = false) {
        return Task.Run(() => CreateIndex(columnNames, unique));
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
        CreateIndex(Name, columnNames, unique);
    }
    /// <inheritdoc cref="CreateIndex(IEnumerable{Expression{Func{T, object}}}, bool)"/>
    public Task CreateIndexAsync(IEnumerable<Expression<Func<T, object>>> properties, bool unique = false) {
        return Task.Run(() => CreateIndex(properties, unique));
    }

    /// <inheritdoc cref="CreateIndex(IEnumerable{Expression{Func{T, object}}}, bool)"/>
    public void CreateIndex(Expression<Func<T, object>> property, bool unique = false) {
        CreateIndex([property], unique);
    }
    /// <inheritdoc cref="CreateIndex(Expression{Func{T, object}}, bool)"/>
    public Task CreateIndexAsync(Expression<Func<T, object>> property, bool unique = false) {
        return Task.Run(() => CreateIndex(property, unique));
    }

    public string MemberNameToColumnName(string memberName) {
        return Columns.First(column => column.ClrMember.Name == memberName).Name;
    }
    public string ColumnNameToMemberName(string columnName) {
        return Columns.First(column => column.Name == columnName).ClrMember.Name;
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
                if (primaryKey is not null) {
                    throw new NotSupportedException("A table cannot have multiple annotated primary keys.");
                }
                primaryKey = column;
            }
        }
        return ([.. columns], primaryKey);
    }
    private void CreateOrMigrateTable() {
        // Check if the table already exists
        List<ColumnInfo> existingColumns = Connection.GetTableInfo(Name).ToList();

        // Create new table
        if (Connection.TableExists(Name)) {
            // Add virtual table modifiers
            string virtualModifier = VirtualModule is not null ? "virtual" : "";
            string usingModifier = VirtualModule is not null ? $"using {VirtualModule.SqlQuote()}" : "";

            // Add column declarations
            string columnDeclarations = string.Join(", ", Columns.Select(Connection.Orm.GetSqlDeclaration));

            // Add without row ID modifier
            string withoutRowIdModifier = WithoutRowId ? "without rowid" : "";

            // Build query
            string query = $"create {virtualModifier} table if not exists {Name.SqlQuote()} {usingModifier} ({columnDeclarations}) {withoutRowIdModifier}";
            // Execute query
            Connection.Execute(query);
        }
        // Migrate existing table
        else {
            List<SqliteColumn> newColumns = Columns.Where(column
                => !existingColumns.Any(existingColumn => existingColumn.Name.Equals(column.Name, StringComparison.OrdinalIgnoreCase))
            ).ToList();

            foreach (SqliteColumn column in newColumns) {
                string sql = $"alter table {Name.SqlQuote()} add column {Connection.Orm.GetSqlDeclaration(column)}";
                Connection.Execute(sql);
            }
        }
    }
    private void CreateIndexes() {
        // Get annotated column indexes
        Dictionary<string, IndexInfo> indexes = [];
        foreach (SqliteColumn column in Columns) {
            foreach (IndexAttribute index in column.Indexes) {
                // Choose name for index
                string indexName = index.Name ?? $"{Name}_{column.Name}";
                // Find index from another column
                if (!indexes.TryGetValue(indexName, out IndexInfo indexInfo)) {
                    indexInfo = new IndexInfo() {
                        IndexName = indexName,
                        TableName = Name,
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
            CreateIndex(index.IndexName, index.Columns, index.Unique);
        }
    }
}

file record struct IndexInfo {
    public string IndexName;
    public string TableName;
    public bool Unique;
    public List<string> Columns;
}