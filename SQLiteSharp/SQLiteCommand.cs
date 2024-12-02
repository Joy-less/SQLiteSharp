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

        Sqlite3Statement stmt = Prepare();

        try {
            Result result = SQLiteRaw.Step(stmt);
            if (result is Result.Row) {
                object? columnValue = Connection.Orm.ReadColumn(stmt, 0, typeof(T));
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
            SQLiteRaw.Finalize(stmt);
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

    /*internal static void BindParameter(Sqlite3Statement stmt, int index, object? value) {
        if (value is null) {
            SQLiteRaw.BindNull(stmt, index);
        }
        else {
            if (value is int intValue) {
                SQLiteRaw.BindInt(stmt, index, intValue);
            }
            else if (value is string stringValue) {
                SQLiteRaw.BindText(stmt, index, stringValue);
            }
            else if (value is byte or sbyte or ushort or ushort) {
                SQLiteRaw.BindInt(stmt, index, Convert.ToInt32(value));
            }
            else if (value is bool boolValue) {
                SQLiteRaw.BindInt(stmt, index, boolValue ? 1 : 0);
            }
            else if (value is uint or long or ulong) {
                SQLiteRaw.BindInt64(stmt, index, Convert.ToInt64(value));
            }
            else if (value is float or double or decimal) {
                SQLiteRaw.BindDouble(stmt, index, Convert.ToDouble(value));
            }
            else if (value is TimeSpan timeSpanValue) {
                SQLiteRaw.BindInt64(stmt, index, timeSpanValue.Ticks);
            }
            else if (value is DateTime dateTimeValue) {
                SQLiteRaw.BindInt64(stmt, index, dateTimeValue.Ticks);
            }
            else if (value is DateTimeOffset dateTimeOffsetValue) {
                SQLiteRaw.BindBlob(stmt, index, DateTimeOffsetToBytes(dateTimeOffsetValue));
            }
            else if (value is byte[] byteArrayValue) {
                SQLiteRaw.BindBlob(stmt, index, byteArrayValue);
            }
            else if (value is Guid or StringBuilder or Uri or UriBuilder) {
                SQLiteRaw.BindText(stmt, index, value.ToString()!);
            }
            else {
                // Now we could possibly get an enum, retrieve cached info
                Type valueType = value.GetType();
                if (valueType.IsEnum) {
                    if (valueType.GetCustomAttribute<StoreByNameAttribute>() is not null) {
                        SQLiteRaw.BindText(stmt, index, Enum.GetName(valueType, value)!);
                    }
                    else {
                        SQLiteRaw.BindInt(stmt, index, Convert.ToInt32(value));
                    }
                }
                else {
                    throw new NotSupportedException($"Cannot store type: {value.GetType()}");
                }
            }
        }
    }*/

    /*private static object? ReadColumn(Sqlite3Statement stmt, int index, ColumnType type, Type clrType) {
        if (type is ColumnType.Null) {
            return null;
        }
        else {
            if (Nullable.GetUnderlyingType(clrType) is Type underlyingType) {
                clrType = underlyingType;
            }

            if (clrType == typeof(string)) {
                return SQLiteRaw.GetColumnText(stmt, index);
            }
            else if (clrType == typeof(int)) {
                return SQLiteRaw.GetColumnInt(stmt, index);
            }
            else if (clrType == typeof(bool)) {
                return SQLiteRaw.GetColumnInt(stmt, index) == 1;
            }
            else if (clrType == typeof(double)) {
                return SQLiteRaw.GetColumnDouble(stmt, index);
            }
            else if (clrType == typeof(float)) {
                return (float)SQLiteRaw.GetColumnDouble(stmt, index);
            }
            else if (clrType == typeof(TimeSpan)) {
                return new TimeSpan(SQLiteRaw.GetColumnInt64(stmt, index));
            }
            else if (clrType == typeof(DateTime)) {
                return new DateTime(SQLiteRaw.GetColumnInt64(stmt, index));
            }
            else if (clrType == typeof(DateTimeOffset)) {
                return BytesToDateTimeOffset(SQLiteRaw.GetColumnBlob(stmt, index));
            }
            else if (clrType.IsEnum) {
                if (type is ColumnType.Text) {
                    string value = SQLiteRaw.GetColumnText(stmt, index);
                    return Enum.Parse(clrType, value, true);
                }
                else {
                    return SQLiteRaw.GetColumnInt(stmt, index);
                }
            }
            else if (clrType == typeof(long)) {
                return SQLiteRaw.GetColumnInt64(stmt, index);
            }
            else if (clrType == typeof(ulong)) {
                return (ulong)SQLiteRaw.GetColumnInt64(stmt, index);
            }
            else if (clrType == typeof(uint)) {
                return (uint)SQLiteRaw.GetColumnInt64(stmt, index);
            }
            else if (clrType == typeof(decimal)) {
                return (decimal)SQLiteRaw.GetColumnDouble(stmt, index);
            }
            else if (clrType == typeof(byte)) {
                return (byte)SQLiteRaw.GetColumnInt(stmt, index);
            }
            else if (clrType == typeof(ushort)) {
                return (ushort)SQLiteRaw.GetColumnInt(stmt, index);
            }
            else if (clrType == typeof(short)) {
                return (short)SQLiteRaw.GetColumnInt(stmt, index);
            }
            else if (clrType == typeof(sbyte)) {
                return (sbyte)SQLiteRaw.GetColumnInt(stmt, index);
            }
            else if (clrType == typeof(byte[])) {
                return SQLiteRaw.GetColumnBlob(stmt, index);
            }
            else if (clrType == typeof(Guid)) {
                string text = SQLiteRaw.GetColumnText(stmt, index);
                return new Guid(text);
            }
            else if (clrType == typeof(Uri)) {
                string text = SQLiteRaw.GetColumnText(stmt, index);
                return new Uri(text);
            }
            else if (clrType == typeof(StringBuilder)) {
                string text = SQLiteRaw.GetColumnText(stmt, index);
                return new StringBuilder(text);
            }
            else if (clrType == typeof(UriBuilder)) {
                string text = SQLiteRaw.GetColumnText(stmt, index);
                return new UriBuilder(text);
            }
            else {
                throw new NotSupportedException("Don't know how to read " + clrType);
            }
        }
    }*/

    internal static DateTimeOffset BytesToDateTimeOffset(byte[] bytes) {
        long dateTicks = BitConverter.ToInt64(bytes, 0);
        long offsetTicks = BitConverter.ToInt64(bytes, sizeof(long));
        return new DateTimeOffset(new DateTime(dateTicks), TimeSpan.FromTicks(offsetTicks));
    }
    internal static byte[] DateTimeOffsetToBytes(DateTimeOffset dateTimeOffset) {
        return [
            .. BitConverter.GetBytes(dateTimeOffset.DateTime.Ticks),
            .. BitConverter.GetBytes(dateTimeOffset.Offset.Ticks)
        ];
    }

    private record struct Parameter(string? Name, object? Value) {
        public string? Name { get; set; } = Name;
        public object? Value { get; set; } = Value;

        public readonly override string ToString() {
            return Name is not null ? $"{Name} = {Value}" : $"{Value}";
        }
    }
}