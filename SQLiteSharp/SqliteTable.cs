using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp;

/// <summary>
/// A single table in a <see cref="SqliteConnection"/> mapped to a CLR object.
/// </summary>
public class SqliteTable<T> where T : notnull, new() {
    /// <summary>
    /// The database connection connected to the table.
    /// </summary>
    public SqliteConnection Connection { get; }
    /// <summary>
    /// The name of the table.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// The virtual module name (e.g. "FTS5") to use when creating the table.
    /// </summary>
    public string? VirtualModule { get; }
    /// <summary>
    /// Whether the table should be created without an implicit <c>rowid</c>.
    /// </summary>
    public bool WithoutRowId { get; }
    /// <summary>
    /// The columns of the table.
    /// </summary>
    public SqliteColumn[] Columns { get; }
    /// <summary>
    /// The column designated as the primary key of the table.
    /// </summary>
    public SqliteColumn? PrimaryKey { get; }

    internal SqliteTable(SqliteConnection connection, string? name = null, string? virtualModule = null, bool createTable = true) {
        Connection = connection;
        Name = name ?? typeof(T).GetCustomAttribute<AliasAttribute>()?.Name ?? typeof(T).Name;
        VirtualModule = virtualModule;
        WithoutRowId = typeof(T).GetCustomAttribute<WithoutRowIdAttribute>() is not null;

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

    /// <summary>
    /// Returns a <see cref="SqlBuilder{T}"/> to build and execute a complex SQL query using the fluent style.
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
    public IEnumerable<T> ExecuteQuery(string query, IReadOnlyDictionary<string, object?> parameters) {
        return Connection.CreateCommand(query, parameters).ExecuteQuery(this);
    }
    /// <inheritdoc cref="ExecuteQuery(string, IEnumerable{object?})"/>
    public IAsyncEnumerable<T> ExecuteQueryAsync(string query, IReadOnlyDictionary<string, object?> parameters) {
        return ExecuteQuery(query, parameters).ToAsyncEnumerable();
    }

    /// <summary>
    /// Retrieves every row in the table.
    /// </summary>
    public IEnumerable<T> FindAll() {
        return Build().Select().ExecuteQuery();
    }
    /// <inheritdoc cref="FindAll()"/>
    public IAsyncEnumerable<T> FindAllAsync() {
        return FindAll().ToAsyncEnumerable();
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
            throw new NotSupportedException($"Can't find by key in table '{Name}' since it has no annotated primary key");
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
    /// Retrieves each row matching the predicate.
    /// </summary>
    /// <returns>
    /// The rows matching the predicate.
    /// </returns>
    public IEnumerable<T> Find(Expression<Func<T, bool>> predicate) {
        return Build().Select().Where(predicate).ExecuteQuery();
    }
    /// <inheritdoc cref="FindOne(Expression{Func{T, bool}})"/>
    public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate) {
        return Find(predicate).ToAsyncEnumerable();
    }

    /// <summary>
    /// Retrieves the first row matching the predicate.
    /// </summary>
    /// <returns>
    /// The first row matching the predicate, or <see langword="null"/> if no rows match the predicate.
    /// </returns>
    public T? FindOne(Expression<Func<T, bool>> predicate) {
        return Find(predicate).FirstOrDefault();
    }
    /// <inheritdoc cref="FindOne(Expression{Func{T, bool}})"/>
    public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => FindOne(predicate));
    }

    /// <summary>
    /// Counts the number of rows matching the predicate.
    /// </summary>
    public long Count(Expression<Func<T, bool>>? predicate = null) {
        SqlBuilder<T> query = Build().Select(SelectType.Count);
        if (predicate is not null) {
            query.Where(predicate);
        }
        return query.ExecuteScalars<long>().First();
    }
    /// <inheritdoc cref="Count(Expression{Func{T, bool}}?)"/>
    public Task<long> CountAsync(Expression<Func<T, bool>>? predicate = null) {
        return Task.Run(() => Count(predicate));
    }

    /// <summary>
    /// Inserts the row into the table, updating any auto-incremented primary keys.<br/>
    /// </summary>
    /// <param name="modifier">
    /// Literal SQL added after <c>INSERT</c>: <c>[OR REPLACE, OR IGNORE, OR ABORT, OR FAIL, OR ROLLBACK]</c>
    /// </param>
    /// <returns>The number of rows added.</returns>
    public int Insert(T row, string? modifier = null) {
        // Build insert query
        SqlBuilder<T> builder = Build();
        foreach (SqliteColumn column in Columns) {
            // Strip auto-incremented columns from insert (unless contains modifier like "OR REPLACE")
            if (column.IsAutoIncremented && modifier is null) {
                continue;
            }
            builder.Insert(column.Name, builder.AddParameter(column.GetValue(row)));
        }

        // Execute insert
        int rowCount = builder.Execute();

        // Auto-increment columns
        foreach (SqliteColumn column in Columns) {
            if (column.IsAutoIncremented) {
                // Get inserted row ID
                long rowId = SqliteRaw.GetLastInsertRowId(Connection.Handle);
                // Map row ID
                column.SetSqliteValue(row, rowId);
            }
        }
        return rowCount;
    }
    /// <inheritdoc cref="Insert(T, string?)"/>
    public Task<int> InsertAsync(T row, string? modifier = null) {
        return Task.Run(() => Insert(row, modifier));
    }

    /// <inheritdoc cref="Insert(T, string?)"/>
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
    public int Update(T row) {
        // Ensure table has primary key
        if (PrimaryKey is null) {
            throw new NotSupportedException($"Can't update in table '{Name}' since it has no annotated primary key");
        }

        // Build update query
        SqlBuilder<T> builder = Build();
        foreach (SqliteColumn column in Columns) {
            // Strip primary key column from update
            if (column.IsPrimaryKey) {
                continue;
            }
            builder.Update(column.Name, builder.AddParameter(column.GetValue(row)));
        }
        // Add primary key condition
        builder.Where($"{PrimaryKey.Name.SqlQuote()} = {builder.AddParameter(PrimaryKey.GetValue(row))}");

        // Execute update
        int rowCount = builder.Execute();
        return rowCount;
    }
    /// <inheritdoc cref="Update(T)"/>
    public Task<int> UpdateAsync(T row) {
        return Task.Run(() => Update(row));
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
                counter += Update(row);
            }
        });
        return counter;
    }
    /// <inheritdoc cref="UpdateAll(IEnumerable{T})"/>
    public Task<int> UpdateAllAsync(IEnumerable<T> rows) {
        return Task.Run(() => UpdateAll(rows));
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
        return Build().Delete().Execute();
    }
    /// <inheritdoc cref="DeleteAll()"/>
    public Task<int> DeleteAllAsync() {
        return Task.Run(DeleteAll);
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

        // Build delete query
        SqlBuilder<T> builder = Build().Delete();
        // Add primary key condition
        builder.Where($"{PrimaryKey.Name.SqlQuote()} = {builder.AddParameter(primaryKey)}");

        // Execute delete
        int rowCount = builder.Execute();
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
    /// Deletes each row matching the predicate.
    /// </summary>
    /// <returns>
    /// The number of rows deleted.
    /// </returns>
    public int Delete(Expression<Func<T, bool>> predicate) {
        return Build().Delete().Where(predicate).Execute();
    }
    /// <inheritdoc cref="Delete(Expression{Func{T, bool}})"/>
    public Task<int> DeleteAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => Delete(predicate));
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

    /// <summary>
    /// Finds the name of the SQLite column mapped to the CLR member with the given name.
    /// </summary>
    public string MemberNameToColumnName(string memberName) {
        return Columns.First(column => column.ClrMember.Name == memberName).Name;
    }
    /// <summary>
    /// Finds the name of the CLR member mapped to the SQLite column with the given name.
    /// </summary>
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
        // Create new table
        if (!Connection.GetTables(Name).Any()) {
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
            // Get columns already in the table
            List<ColumnInfo> existingColumns = Connection.GetColumns(Name).ToList();

            // Get new columns to add
            List<SqliteColumn> newColumns = Columns.Where(
                column => !existingColumns.Any(existingColumn => existingColumn.Name.Equals(column.Name, StringComparison.OrdinalIgnoreCase))
            ).ToList();

            // Add new columns
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