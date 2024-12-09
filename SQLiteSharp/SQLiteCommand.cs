namespace SQLiteSharp;

/// <summary>
/// A command to be executed in the database containing raw SQL and parameters.
/// </summary>
public class SqliteCommand(SqliteConnection connection) {
    /// <summary>
    /// The database connection connected to the command.
    /// </summary>
    public SqliteConnection Connection { get; } = connection;
    /// <summary>
    /// The raw SQL query.
    /// </summary>
    public string CommandText { get; set; } = "";
    /// <summary>
    /// The parameters included in the command text.
    /// </summary>
    public IEnumerable<SqliteCommandParameter> Parameters { get; set; } = [];

    /// <summary>
    /// Invoked when an object is created to map a row of a table.
    /// </summary>
    public event Action<object>? OnInstanceCreated;

    /// <summary>
    /// Describes the command and its parameters.
    /// </summary>
    public override string ToString() {
        return $"{CommandText} [{string.Join(", ", Parameters)}]";
    }

    /// <summary>
    /// Runs the command in the database.
    /// </summary>
    /// <returns>
    /// The number of rows added/modified.
    /// </returns>
    public int Execute() {
        Sqlite3Statement statement = Prepare();
        Result result = SqliteRaw.Step(statement);
        SqliteRaw.Finalize(statement);

        if (result is Result.Done) {
            int rowCount = SqliteRaw.Changes(Connection.Handle);
            return rowCount;
        }
        else {
            throw new SqliteException(result, SqliteRaw.GetErrorMessage(Connection.Handle));
        }
    }
    /// <summary>
    /// Runs the command in the database.
    /// </summary>
    /// <returns>
    /// A primitive value for the first column of each row returned by the command.
    /// </returns>
    public IEnumerable<T> ExecuteScalars<T>() {
        Sqlite3Statement statement = Prepare();
        try {
            while (true) {
                Result result = SqliteRaw.Step(statement);

                if (result is Result.Row) {
                    object? value = ReadColumn(statement, 0, typeof(T));
                    yield return value is not null ? (T)value : default!;
                }
                else if (result is Result.Done) {
                    break;
                }
                else {
                    throw new SqliteException(result, SqliteRaw.GetErrorMessage(Connection.Handle));
                }
            }
        }
        finally {
            SqliteRaw.Finalize(statement);
        }
    }
    /// <summary>
    /// Runs the command in the database.
    /// </summary>
    /// <returns>
    /// An object for each row returned by the command.
    /// </returns>
    public IEnumerable<T> ExecuteQuery<T>(SqliteTable<T> table) where T : notnull, new() {
        Sqlite3Statement statement = Prepare();
        try {
            while (SqliteRaw.Step(statement) is Result.Row) {
                // Create row object
                T row = new();

                // Iterate through found columns
                int columnCount = SqliteRaw.GetColumnCount(statement);
                for (int i = 0; i < columnCount; i++) {
                    // Get name of found column
                    string columnName = SqliteRaw.GetColumnName(statement, i);

                    // Find mapped column with same name
                    if (table.Columns.FirstOrDefault(column => column.Name == columnName) is not SqliteColumn column) {
                        continue;
                    }

                    // Read value from found column
                    object? value = ReadColumn(statement, i, column.ClrType);
                    column.SetValue(row, value);
                }

                // Return row object
                OnInstanceCreated?.Invoke(row);
                yield return row;
            }
        }
        finally {
            SqliteRaw.Finalize(statement);
        }
    }

    private Sqlite3Statement Prepare() {
        Sqlite3Statement statement = SqliteRaw.Prepare(Connection.Handle, CommandText);
        BindParameters(statement);
        return statement;
    }
    private void BindParameters(Sqlite3Statement statement) {
        int nextIndex = 1;
        foreach ((string? name, object? value) in Parameters) {
            int index = name is not null
                ? SqliteRaw.BindParameterIndex(statement, name)
                : nextIndex++;
            BindParameter(statement, index, value);
        }
    }
    private void BindParameter(Sqlite3Statement statement, int index, object? value) {
        if (value is null) {
            SqliteRaw.BindNull(statement, index);
            return;
        }

        TypeSerializer typeSerializer = Connection.Orm.GetTypeSerializer(value.GetType());
        SqliteValue rawValue = typeSerializer.Serialize(value);

        switch (rawValue.SqliteType) {
            case SqliteType.Null:
                SqliteRaw.BindNull(statement, index);
                break;
            case SqliteType.Integer:
                SqliteRaw.BindInt64(statement, index, rawValue.AsInteger);
                break;
            case SqliteType.Float:
                SqliteRaw.BindDouble(statement, index, rawValue.AsFloat);
                break;
            case SqliteType.Text:
                SqliteRaw.BindText(statement, index, rawValue.AsText);
                break;
            case SqliteType.Blob:
                SqliteRaw.BindBlob(statement, index, rawValue.AsBlob);
                break;
            default:
                throw new NotImplementedException($"Cannot bind column type '{rawValue.SqliteType}'");
        }
    }
    private object? ReadColumn(Sqlite3Statement statement, int index, Type type) {
        return Connection.Orm.Deserialize(SqliteRaw.GetColumnValue(statement, index), type);
    }
}

/// <summary>
/// A parameter for a <see cref="SqliteCommand"/> with an optional name.
/// </summary>
public record struct SqliteCommandParameter(string? Name, object? Value) {
    /// <summary>
    /// The optional name for the parameter.
    /// </summary>
    /// <remarks>
    /// If not provided, the parameter's index is used instead.
    /// </remarks>
    public string? Name { get; set; } = Name;
    /// <summary>
    /// The CLR value of the parameter.
    /// </summary>
    public object? Value { get; set; } = Value;

    /// <summary>
    /// Describes the command parameter.
    /// </summary>
    public readonly override string ToString() {
        return Name is not null ? $"{Name} = {Value}" : $"{Value}";
    }
}