namespace SQLiteSharp;

public class SQLiteCommand(SQLiteConnection connection) {
    public SQLiteConnection Connection { get; } = connection;
    public string CommandText { get; set; } = "";

    public event Action<object>? OnInstanceCreated;

    private readonly List<Parameter> Parameters = [];

    public override string ToString() {
        return $"{CommandText} [{string.Join(", ", Parameters)}]";
    }

    public int ExecuteNonQuery() {
        Sqlite3Statement statement = Prepare();
        Result result = SQLiteRaw.Step(statement);
        SQLiteRaw.Finalize(statement);

        switch (result) {
            case Result.Done:
                int rowCount = SQLiteRaw.Changes(Connection.Handle);
                return rowCount;
            case Result.Constraint when SQLiteRaw.GetExtendedErrorCode(Connection.Handle) is ExtendedResult.ConstraintNotNull:
                throw new NotNullConstraintViolationException(result, SQLiteRaw.GetErrorMessage(Connection.Handle));
            default:
                throw new SQLiteException(result, SQLiteRaw.GetErrorMessage(Connection.Handle));
        }
    }
    public IEnumerable<object> ExecuteQuery(TableMap map) {
        Sqlite3Statement statement = Prepare();
        try {
            while (SQLiteRaw.Step(statement) is Result.Row) {
                object obj = Activator.CreateInstance(map.Type)!;

                // Iterate through found columns
                int columnCount = SQLiteRaw.GetColumnCount(statement);
                for (int i = 0; i < columnCount; i++) {
                    // Get name of found column
                    string columnName = SQLiteRaw.GetColumnName(statement, i);
                    // Find mapped column with same name
                    if (map.FindColumnByColumnName(columnName) is not ColumnMap column) {
                        continue;
                    }
                    // Read value from found column
                    object? value = Connection.Orm.ReadColumn(statement, i, column.ClrType);
                    column.SetValue(obj, value);
                }
                OnInstanceCreated?.Invoke(obj);
                yield return obj;
            }
        }
        finally {
            SQLiteRaw.Finalize(statement);
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
            Result result = SQLiteRaw.Step(statement);
            if (result is Result.Row) {
                object? columnValue = Connection.Orm.ReadColumn(statement, 0, typeof(T));
                if (columnValue is not null) {
                    Value = (T)columnValue;
                }
            }
            else if (result is Result.Done) {
            }
            else {
                throw new SQLiteException(result, SQLiteRaw.GetErrorMessage(Connection.Handle));
            }
        }
        finally {
            SQLiteRaw.Finalize(statement);
        }

        return Value;
    }
    public IEnumerable<T> ExecuteQueryScalars<T>() {
        Sqlite3Statement statement = Prepare();
        try {
            if (SQLiteRaw.GetColumnCount(statement) < 1) {
                throw new InvalidOperationException("QueryScalars should return at least one column");
            }
            while (SQLiteRaw.Step(statement) is Result.Row) {
                object? value = Connection.Orm.ReadColumn(statement, 0, typeof(T));
                if (value is null) {
                    yield return default!;
                }
                else {
                    yield return (T)value;
                }
            }
        }
        finally {
            SQLiteRaw.Finalize(statement);
        }
    }

    public void AddParameter(string? name, object? value) {
        Parameters.Add(new Parameter(name, value));
    }

    private Sqlite3Statement Prepare() {
        Sqlite3Statement statement = SQLiteRaw.Prepare(Connection.Handle, CommandText);
        BindParameters(statement);
        return statement;
    }
    private void BindParameters(Sqlite3Statement statement) {
        int nextIndex = 1;
        foreach ((string? name, object? value) in Parameters) {
            int index = name is not null
                ? SQLiteRaw.BindParameterIndex(statement, name)
                : nextIndex++;
            Connection.Orm.BindParameter(statement, index, value);
        }
    }

    private record struct Parameter(string? Name, object? Value) {
        public string? Name { get; set; } = Name;
        public object? Value { get; set; } = Value;

        public readonly override string ToString() {
            return Name is not null ? $"{Name} = {Value}" : $"{Value}";
        }
    }
}