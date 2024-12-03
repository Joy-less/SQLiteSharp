namespace SQLiteSharp;

public class SqliteCommand(SqliteConnection connection) {
    public SqliteConnection Connection { get; } = connection;
    public string CommandText { get; set; } = "";
    public IEnumerable<SqliteCommandParameter> Parameters { get; set; } = [];

    public event Action<object>? OnInstanceCreated;

    public override string ToString() {
        return $"{CommandText} [{string.Join(", ", Parameters)}]";
    }

    public int ExecuteNonQuery() {
        Sqlite3Statement statement = Prepare();
        Result result = SqliteRaw.Step(statement);
        SqliteRaw.Finalize(statement);

        switch (result) {
            case Result.Done:
                int rowCount = SqliteRaw.Changes(Connection.Handle);
                return rowCount;
            case Result.Constraint when SqliteRaw.GetExtendedErrorCode(Connection.Handle) is ExtendedResult.ConstraintNotNull:
                throw new NotNullConstraintViolationException(result, SqliteRaw.GetErrorMessage(Connection.Handle));
            default:
                throw new SqliteException(result, SqliteRaw.GetErrorMessage(Connection.Handle));
        }
    }
    public IEnumerable<T> ExecuteQuery<T>(SqliteTable<T> table) where T : new() {
        Sqlite3Statement statement = Prepare();
        try {
            while (SqliteRaw.Step(statement) is Result.Row) {
                // Create object to map
                T obj = new();

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
                    column.SetValue(obj, value);
                }

                // Return mapped object
                OnInstanceCreated?.Invoke(obj);
                yield return obj;
            }
        }
        finally {
            SqliteRaw.Finalize(statement);
        }
    }
    public T ExecuteScalar<T>() {
        T Value = default!;

        Sqlite3Statement statement = Prepare();

        try {
            Result result = SqliteRaw.Step(statement);
            if (result is Result.Row) {
                object? columnValue = ReadColumn(statement, 0, typeof(T));
                if (columnValue is not null) {
                    Value = (T)columnValue;
                }
            }
            else if (result is Result.Done) {
            }
            else {
                throw new SqliteException(result, SqliteRaw.GetErrorMessage(Connection.Handle));
            }
        }
        finally {
            SqliteRaw.Finalize(statement);
        }

        return Value;
    }
    public IEnumerable<T> ExecuteQueryScalars<T>() {
        Sqlite3Statement statement = Prepare();
        try {
            if (SqliteRaw.GetColumnCount(statement) < 1) {
                throw new InvalidOperationException("QueryScalars should return at least one column");
            }
            while (SqliteRaw.Step(statement) is Result.Row) {
                object? value = ReadColumn(statement, 0, typeof(T));
                if (value is null) {
                    yield return default!;
                }
                else {
                    yield return (T)value;
                }
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