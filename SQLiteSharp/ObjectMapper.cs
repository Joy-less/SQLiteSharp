using System.Text;
using System.Reflection;

namespace SQLiteSharp;

public class ObjectMapper {
    public const string ImplicitPrimaryKeyName = "Id";
    public const string ImplicitIndexSuffix = "Id";

    public static string GetSqlDeclaration(TableMapping.Column column) {
        string declaration = $"{Quote(column.Name)} {Quote(GetSqlType(column))} collate {Quote(column.Collation)} ";

        if (column.PrimaryKey) {
            declaration += "primary key ";
        }
        if (column.AutoIncrement) {
            declaration += "autoincrement ";
        }
        if (column.NotNull) {
            declaration += "not null ";
        }

        return declaration;
    }
    public static string GetSqlType(TableMapping.Column column) {
        Type clrType = column.Type;
        if (clrType == typeof(bool) || clrType == typeof(byte) || clrType == typeof(sbyte) || clrType == typeof(short) || clrType == typeof(ushort) || clrType == typeof(int) || clrType == typeof(uint) || clrType == typeof(long) || clrType == typeof(ulong)) {
            return "integer";
        }
        else if (clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal)) {
            return "float";
        }
        else if (clrType == typeof(string) || clrType == typeof(StringBuilder) || clrType == typeof(Uri) || clrType == typeof(UriBuilder)) {
            if (column.MaxStringLength is int maxStringLength) {
                return "varchar(" + maxStringLength + ")";
            }
            return "varchar";
        }
        else if (clrType == typeof(TimeSpan)) {
            return "bigint";
        }
        else if (clrType == typeof(DateTime)) {
            return "bigint";
        }
        else if (clrType == typeof(DateTimeOffset)) {
            return "blob";
        }
        else if (clrType.IsEnum) {
            return column.StoreAsText ? "varchar" : "integer";
        }
        else if (clrType == typeof(byte[])) {
            return "blob";
        }
        else if (clrType == typeof(Guid)) {
            return "varchar(36)";
        }
        else {
            throw new NotSupportedException("Don't know about " + clrType);
        }
    }
    public static bool IsPrimaryKey(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<PrimaryKeyAttribute>() is not null;
    }
    public static bool IsAutoIncrement(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<AutoIncrementAttribute>() is not null;
    }
    public static string GetCollation(MemberInfo memberInfo) {
		return memberInfo.GetCustomAttribute<CollationAttribute>()?.Value ?? CollationType.Binary;
    }

    public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttributes<IndexedAttribute>();
    }
    public static int? MaxStringLength(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<MaxLengthAttribute>()?.Value;
    }

    public static bool IsMarkedNotNull(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<NotNullAttribute>() is not null;
    }
}

public partial class SQLiteCommand(SQLiteConnection conn) {
    private readonly SQLiteConnection _conn = conn;
    private readonly List<Binding> _bindings = [];

    public string CommandText { get; set; } = "";

    public int ExecuteNonQuery() {
        Sqlite3Statement statement = Prepare();
        SQLiteRaw.Result result = SQLiteRaw.Step(statement);
        SQLiteRaw.Finalize(statement);

        switch (result) {
            case SQLiteRaw.Result.Done:
                int rowCount = SQLiteRaw.Changes(_conn.Handle);
                return rowCount;
            case SQLiteRaw.Result.Error:
                string errorMessage = SQLiteRaw.GetErrorMessage(_conn.Handle);
                throw new SQLiteException(result, errorMessage);
            case SQLiteRaw.Result.Constraint when SQLiteRaw.GetExtendedErrorCode(_conn.Handle) is SQLiteRaw.ExtendedResult.ConstraintNotNull:
                throw new NotNullConstraintViolationException(result, SQLiteRaw.GetErrorMessage(_conn.Handle));
            default:
                throw new SQLiteException(result, SQLiteRaw.GetErrorMessage(_conn.Handle));
        }
    }

    /// <summary>
    /// Invoked every time an instance is loaded from the database.
    /// </summary>
    /// <param name='obj'>
    /// The newly created object.
    /// </param>
    /// <remarks>
    /// This can be overridden in combination with the <see cref="SQLiteConnection.NewCommand"/> method to hook into the life-cycle of objects.
    /// </remarks>
    protected virtual void OnInstanceCreated(object obj) { }

    public IEnumerable<T> ExecuteQuery<T>(TableMapping map) {
        Sqlite3Statement statement = Prepare();
        try {
            while (SQLiteRaw.Step(statement) is SQLiteRaw.Result.Row) {
                object obj = Activator.CreateInstance(map.MappedType)!;

                // Iterate through found columns
                int columnCount = SQLiteRaw.GetColumnCount(statement);
                for (int i = 0; i < columnCount; i++) {
                    // Get name of found column
                    string columnName = SQLiteRaw.GetColumnName(statement, i);
                    // Find mapped column with same name
                    if (map.FindColumnByColumnName(columnName) is not TableMapping.Column column) {
                        continue;
                    }
                    // Read value from found column
                    SQLiteRaw.ColumnType columnType = SQLiteRaw.GetColumnType(statement, i);
                    object? value = ReadColumn(statement, i, columnType, column.Type);
                    column.SetValue(obj, value);
                }
                OnInstanceCreated(obj);
                yield return (T)obj;
            }
        }
        finally {
            SQLiteRaw.Finalize(statement);
        }
    }
    public IEnumerable<T> ExecuteQuery<T>() {
        return ExecuteQuery<T>(_conn.GetMapping<T>());
    }

    public T ExecuteScalar<T>() {
        T Value = default!;

        Sqlite3Statement stmt = Prepare();

        try {
            SQLiteRaw.Result result = SQLiteRaw.Step(stmt);
            if (result is SQLiteRaw.Result.Row) {
                SQLiteRaw.ColumnType columnType = SQLiteRaw.GetColumnType(stmt, 0);
                object? columnValue = ReadColumn(stmt, 0, columnType, typeof(T));
                if (columnValue is not null) {
                    Value = (T)columnValue;
                }
            }
            else if (result is SQLiteRaw.Result.Done) {
            }
            else {
                throw new SQLiteException(result, SQLiteRaw.GetErrorMessage(_conn.Handle));
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
            while (SQLiteRaw.Step(statement) == SQLiteRaw.Result.Row) {
                SQLiteRaw.ColumnType colType = SQLiteRaw.GetColumnType(statement, 0);
                object? value = ReadColumn(statement, 0, colType, typeof(T));
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

    public void Bind(string? name, object? value) {
        _bindings.Add(new Binding() {
            Name = name,
            Value = value
        });
    }
    public void Bind(object? value) {
        Bind(null, value);
    }

    public override string ToString() {
        StringBuilder builder = new();
        builder.AppendLine(CommandText);
        int i = 0;
        foreach (Binding binding in _bindings) {
            builder.AppendLine($" {i}: {binding.Value}");
            i++;
        }
        return builder.ToString();
    }

    private Sqlite3Statement Prepare() {
        Sqlite3Statement stmt = SQLiteRaw.Prepare2(_conn.Handle, CommandText);
        BindAll(stmt);
        return stmt;
    }

    private void BindAll(Sqlite3Statement stmt) {
        int nextIndex = 1;
        foreach (Binding binding in _bindings) {
            if (binding.Name is not null) {
                binding.Index = SQLiteRaw.BindParameterIndex(stmt, binding.Name);
            }
            else {
                binding.Index = nextIndex++;
            }
            BindParameter(stmt, binding.Index, binding.Value);
        }
    }

    internal static void BindParameter(Sqlite3Statement stmt, int index, object? value) {
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
    }

    private class Binding {
        public string? Name { get; set; }
        public object? Value { get; set; }
        public int Index { get; set; }
    }

    private static object? ReadColumn(Sqlite3Statement stmt, int index, SQLiteRaw.ColumnType type, Type clrType) {
        if (type is SQLiteRaw.ColumnType.Null) {
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
                if (type is SQLiteRaw.ColumnType.Text) {
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
    }

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
}