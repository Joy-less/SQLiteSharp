namespace SQLiteSharp;

public class SqliteCommand(SqliteConnection connection) {
    public SqliteConnection Connection { get; } = connection;
    public string CommandText { get; set; } = "";

    public event Action<object>? OnInstanceCreated;

    private readonly List<Parameter> Parameters = [];

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
    public IEnumerable<object> ExecuteQuery(TableMap map) {
        Sqlite3Statement statement = Prepare();
        try {
            while (SqliteRaw.Step(statement) is Result.Row) {
                object obj = Activator.CreateInstance(map.Type)!;

                // Iterate through found columns
                int columnCount = SqliteRaw.GetColumnCount(statement);
                for (int i = 0; i < columnCount; i++) {
                    // Get name of found column
                    string columnName = SqliteRaw.GetColumnName(statement, i);
                    // Find mapped column with same name
                    if (map.FindColumnByColumnName(columnName) is not ColumnMap column) {
                        continue;
                    }
                    // Read value from found column
                    object? value = ReadColumn(statement, i, column.ClrType);
                    column.SetValue(obj, value);
                }
                OnInstanceCreated?.Invoke(obj);
                yield return obj;
            }
        }
        finally {
            SqliteRaw.Finalize(statement);
        }
    }
    public IEnumerable<T> ExecuteQuery<T>(TableMap map) {
        return ExecuteQuery(map).Cast<T>();
    }
    public IEnumerable<T> ExecuteQuery<T>() {
        return ExecuteQuery<T>(Connection.MapTable<T>());
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

    public void AddParameter(string? name, object? value) {
        Parameters.Add(new Parameter(name, value));
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

    private record struct Parameter(string? Name, object? Value) {
        public string? Name { get; set; } = Name;
        public object? Value { get; set; } = Value;

        public readonly override string ToString() {
            return Name is not null ? $"{Name} = {Value}" : $"{Value}";
        }
    }
}