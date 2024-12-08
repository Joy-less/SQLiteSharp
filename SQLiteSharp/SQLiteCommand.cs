namespace SQLiteSharp;

public class SqliteCommand(SqliteConnection connection) {
    public SqliteConnection Connection { get; } = connection;
    public string CommandText { get; set; } = "";
    public IEnumerable<SqliteCommandParameter> Parameters { get; set; } = [];

    public event Action<object>? OnInstanceCreated;

    public override string ToString() {
        return $"{CommandText} [{string.Join(", ", Parameters)}]";
    }

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
        TypeSerializer typeSerializer = Connection.Orm.GetTypeSerializer(type);
        SqliteValue value = SqliteRaw.GetColumnValue(statement, index);
        return typeSerializer.Deserialize(value, type);
    }
}

public record struct SqliteCommandParameter(string? Name, object? Value) {
    public string? Name { get; set; } = Name;
    public object? Value { get; set; } = Value;

    public readonly override string ToString() {
        return Name is not null ? $"{Name} = {Value}" : $"{Value}";
    }
}