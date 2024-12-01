using System.Collections;
using System.Text;
using System.Reflection;
using System.Linq.Expressions;

namespace SQLiteSharp;

public class TableMapping {
    public Type MappedType { get; }
    public string TableName { get; }
    public bool WithoutRowId { get; }
    public Column[] Columns { get; }
    public Column? PrimaryKey { get; }
    public string GetByPrimaryKeySql { get; }
    public CreateFlags CreateFlags { get; }

    internal MapMethod Method { get; } = MapMethod.ByName;

    private readonly Column? _autoIncrementedPrimaryKey;

    public TableMapping(Type type, CreateFlags createFlags = CreateFlags.None) {
        MappedType = type;
        CreateFlags = createFlags;

        TableAttribute? tableAttribute = type.GetCustomAttribute<TableAttribute>();

        TableName = !string.IsNullOrEmpty(tableAttribute?.Name) ? tableAttribute!.Name : MappedType.Name;
        WithoutRowId = tableAttribute is not null && tableAttribute.WithoutRowId;

        MemberInfo[] members = [.. type.GetProperties(), .. type.GetFields()];
        List<Column> columns = new(members.Length);
        foreach (MemberInfo member in members) {
            bool ignore = member.GetCustomAttribute<IgnoreAttribute>() is not null;
            if (!ignore) {
                columns.Add(new Column(member, createFlags));
            }
        }
        Columns = [.. columns];
        foreach (Column column in Columns) {
            if (column.AutoIncrement && column.PrimaryKey) {
                _autoIncrementedPrimaryKey = column;
            }
            if (column.PrimaryKey) {
                PrimaryKey = column;
            }
        }

        if (PrimaryKey is not null) {
            GetByPrimaryKeySql = $"select * from \"{TableName}\" where \"{PrimaryKey.Name}\" = ?";
        }
        else {
            // People should not be calling Get/Find without a primary key
            GetByPrimaryKeySql = $"select * from \"{TableName}\" limit 1";
        }
    }

    public bool HasAutoIncrementedPrimaryKey => _autoIncrementedPrimaryKey is not null;
    public void SetAutoIncrementedPrimaryKey(object obj, long id) {
        _autoIncrementedPrimaryKey?.SetValue(obj, Convert.ChangeType(id, _autoIncrementedPrimaryKey.ColumnType));
    }

    public Column? FindColumnWithPropertyName(string propertyName) {
        return Columns.FirstOrDefault(column => column.PropertyName == propertyName);
    }
    public Column? FindColumn(string columnName) {
        if (Method is not MapMethod.ByName) {
            throw new InvalidOperationException($"This {nameof(TableMapping)} is not mapped by name, but {Method}.");
        }
        return Columns.FirstOrDefault(column => column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
    }

    public class Column {
        public string Name { get; }
        public PropertyInfo? PropertyInfo => _memberInfo as PropertyInfo;
        public string PropertyName { get => _memberInfo.Name; }
        public Type ColumnType { get; }
        public string Collation { get; }
        public bool AutoIncrement { get; }
        public bool AutoGuid { get; }
        public bool PrimaryKey { get; }
        public bool NotNull { get; }
        public int? MaxStringLength { get; }
        public bool StoreAsText { get; }
        public IEnumerable<IndexedAttribute> Indices { get; }

        private readonly MemberInfo _memberInfo;

        public Column(MemberInfo member, CreateFlags createFlags = CreateFlags.None) {
            _memberInfo = member;
            Type memberType = GetMemberType(member);

            Name = member.GetCustomAttribute<ColumnAttribute>()?.Name ?? member.Name;

            // If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
            ColumnType = Nullable.GetUnderlyingType(memberType) ?? memberType;
            Collation = Orm.GetCollation(member);

            PrimaryKey = Orm.IsPrimaryKey(member)
                || (createFlags.HasFlag(CreateFlags.ImplicitPrimaryKey) && string.Equals(member.Name, Orm.ImplicitPrimaryKeyName, StringComparison.OrdinalIgnoreCase));

            bool isAutoIncrement = Orm.IsAutoIncrement(member) || (PrimaryKey && ((createFlags & CreateFlags.AutoIncrementPrimaryKey) == CreateFlags.AutoIncrementPrimaryKey));
            AutoGuid = isAutoIncrement && ColumnType == typeof(Guid);
            AutoIncrement = isAutoIncrement && !AutoGuid;

            Indices = Orm.GetIndices(member);
            if (!Indices.Any() && !PrimaryKey && createFlags.HasFlag(CreateFlags.ImplicitIndex) && Name.EndsWith(Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)) {
                Indices = [new IndexedAttribute()];
            }
            NotNull = PrimaryKey || Orm.IsMarkedNotNull(member);
            MaxStringLength = Orm.MaxStringLength(member);

            StoreAsText = memberType.GetCustomAttribute<StoreByNameAttribute>() is not null;
        }

        public void SetValue(object obj, object? value) {
            if (_memberInfo is PropertyInfo propertyInfo) {
                if (value is not null && ColumnType.IsEnum) {
                    propertyInfo.SetValue(obj, Enum.ToObject(ColumnType, value));
                }
                else {
                    propertyInfo.SetValue(obj, value);
                }
            }
            else if (_memberInfo is FieldInfo fieldInfo) {
                if (value is not null && ColumnType.IsEnum) {
                    fieldInfo.SetValue(obj, Enum.ToObject(ColumnType, value));
                }
                else {
                    fieldInfo.SetValue(obj, value);
                }
            }
            else {
                throw new InvalidProgramException("Unreachable condition");
            }
        }
        public object? GetValue(object obj) {
            if (_memberInfo is PropertyInfo propertyInfo) {
                return propertyInfo.GetValue(obj);
            }
            else if (_memberInfo is FieldInfo fieldInfo) {
                return fieldInfo.GetValue(obj);
            }
            else {
                throw new InvalidProgramException("Unreachable condition");
            }
        }
        private static Type GetMemberType(MemberInfo memberInfo) {
            return memberInfo switch {
                PropertyInfo propertyInfo => propertyInfo.PropertyType,
                FieldInfo fieldInfo => fieldInfo.FieldType,
                _ => throw new InvalidProgramException($"{nameof(TableMapping)} only supports properties and fields."),
            };
        }
    }

    internal enum MapMethod {
        ByName,
        ByPosition
    }
}

public static class Orm {
    public const string ImplicitPrimaryKeyName = "Id";
    public const string ImplicitIndexSuffix = "Id";

    public static string GetSqlDeclaration(TableMapping.Column column) {
        string declaration = $"\"{column.Name}\" {GetSqlType(column)} ";

        if (column.PrimaryKey) {
            declaration += "primary key ";
        }
        if (column.AutoIncrement) {
            declaration += "autoincrement ";
        }
        if (column.NotNull) {
            declaration += "not null ";
        }
        if (!string.IsNullOrEmpty(column.Collation)) {
            declaration += "collate " + column.Collation + " ";
        }

        return declaration;
    }
    public static string GetSqlType(TableMapping.Column column) {
        Type clrType = column.ColumnType;
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
		return memberInfo.GetCustomAttribute<CollationAttribute>()?.Value ?? "";
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
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing: " + this);
        }

        Sqlite3Statement statement = Prepare();
        SQLiteInterop.Result result = SQLiteInterop.Step(statement);
        SQLiteInterop.Finalize(statement);

        if (result is SQLiteInterop.Result.Done) {
            int rowCount = SQLiteInterop.Changes(_conn.Handle!);
            return rowCount;
        }
        else if (result is SQLiteInterop.Result.Error) {
            string msg = SQLiteInterop.GetErrmsg(_conn.Handle!);
            throw new SQLiteException(result, msg);
        }
        else if (result is SQLiteInterop.Result.Constraint) {
            if (SQLiteInterop.ExtendedErrCode(_conn.Handle!) is SQLiteInterop.ExtendedResult.ConstraintNotNull) {
                throw new NotNullConstraintViolationException(result, SQLiteInterop.GetErrmsg(_conn.Handle!));
            }
        }
        throw new SQLiteException(result, SQLiteInterop.GetErrmsg(_conn.Handle!));
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
    protected virtual void OnInstanceCreated(object obj) {
        // Can be overridden.
    }

    public IEnumerable<T> ExecuteQuery<T>(TableMapping map) {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing Query: " + this);
        }

        Sqlite3Statement statement = Prepare();
        try {
            TableMapping.Column?[] columns = new TableMapping.Column[SQLiteInterop.ColumnCount(statement)];
            Action<object, Sqlite3Statement, int>?[] fastColumnSetters = new Action<object, Sqlite3Statement, int>?[SQLiteInterop.ColumnCount(statement)];

            if (map.Method is TableMapping.MapMethod.ByPosition) {
                Array.Copy(map.Columns, columns, Math.Min(columns.Length, map.Columns.Length));
            }
            else if (map.Method is TableMapping.MapMethod.ByName) {
                MethodInfo? getSetter = null;
                if (typeof(T) != map.MappedType) {
                    getSetter = typeof(FastColumnSetter)
                        .GetMethod(nameof(FastColumnSetter.GetFastSetter), BindingFlags.NonPublic | BindingFlags.Static)!
                        .MakeGenericMethod(map.MappedType);
                }

                for (int i = 0; i < columns.Length; i++) {
                    string name = SQLiteInterop.ColumnName16(statement, i);
                    columns[i] = map.FindColumn(name);
                    if (columns[i] is TableMapping.Column column) {
                        if (getSetter is not null) {
                            fastColumnSetters[i] = (Action<object, Sqlite3Statement, int>)getSetter.Invoke(null, [column])!;
                        }
                        else {
                            fastColumnSetters[i] = FastColumnSetter.GetFastSetter<T>(column);
                        }
                    }
                }
            }

            while (SQLiteInterop.Step(statement) is SQLiteInterop.Result.Row) {
                object obj = Activator.CreateInstance(map.MappedType)!;
                for (int i = 0; i < columns.Length; i++) {
                    if (columns[i] is not TableMapping.Column column) {
                        continue;
                    }

                    if (fastColumnSetters[i] is Action<object, Sqlite3Statement, int> fastColumnSetter) {
                        fastColumnSetter.Invoke(obj, statement, i);
                    }
                    else {
                        SQLiteInterop.ColType columnType = SQLiteInterop.ColumnType(statement, i);
                        object? value = ReadCol(statement, i, columnType, column.ColumnType);
                        column.SetValue(obj, value);
                    }
                }
                OnInstanceCreated(obj);
                yield return (T)obj;
            }
        }
        finally {
            SQLiteInterop.Finalize(statement);
        }
    }
    public IEnumerable<T> ExecuteQuery<T>() {
        return ExecuteQuery<T>(_conn.GetMapping<T>());
    }

    public T ExecuteScalar<T>() {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing Query: " + this);
        }

        T Value = default!;

        Sqlite3Statement stmt = Prepare();

        try {
            SQLiteInterop.Result result = SQLiteInterop.Step(stmt);
            if (result is SQLiteInterop.Result.Row) {
                SQLiteInterop.ColType columnType = SQLiteInterop.ColumnType(stmt, 0);
                object? columnValue = ReadCol(stmt, 0, columnType, typeof(T));
                if (columnValue is not null) {
                    Value = (T)columnValue;
                }
            }
            else if (result is SQLiteInterop.Result.Done) {
            }
            else {
                throw new SQLiteException(result, SQLiteInterop.GetErrmsg(_conn.Handle!));
            }
        }
        finally {
            SQLiteInterop.Finalize(stmt);
        }

        return Value;
    }

    public IEnumerable<T> ExecuteQueryScalars<T>() {
        if (_conn.Trace) {
            _conn.Tracer?.Invoke("Executing Query: " + this);
        }
        Sqlite3Statement statement = Prepare();
        try {
            if (SQLiteInterop.ColumnCount(statement) < 1) {
                throw new InvalidOperationException("QueryScalars should return at least one column");
            }
            while (SQLiteInterop.Step(statement) == SQLiteInterop.Result.Row) {
                SQLiteInterop.ColType colType = SQLiteInterop.ColumnType(statement, 0);
                object? value = ReadCol(statement, 0, colType, typeof(T));
                if (value is null) {
                    yield return default!;
                }
                else {
                    yield return (T)value;
                }
            }
        }
        finally {
            SQLiteInterop.Finalize(statement);
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
        Sqlite3Statement stmt = SQLiteInterop.Prepare2(_conn.Handle!, CommandText);
        BindAll(stmt);
        return stmt;
    }

    private void BindAll(Sqlite3Statement stmt) {
        int nextIndex = 1;
        foreach (Binding binding in _bindings) {
            if (binding.Name is not null) {
                binding.Index = SQLiteInterop.BindParameterIndex(stmt, binding.Name);
            }
            else {
                binding.Index = nextIndex++;
            }
            BindParameter(stmt, binding.Index, binding.Value);
        }
    }

    internal static void BindParameter(Sqlite3Statement stmt, int index, object? value) {
        if (value is null) {
            SQLiteInterop.BindNull(stmt, index);
        }
        else {
            if (value is int intValue) {
                SQLiteInterop.BindInt(stmt, index, intValue);
            }
            else if (value is string stringValue) {
                SQLiteInterop.BindText(stmt, index, stringValue);
            }
            else if (value is byte or sbyte or ushort or ushort) {
                SQLiteInterop.BindInt(stmt, index, Convert.ToInt32(value));
            }
            else if (value is bool boolValue) {
                SQLiteInterop.BindInt(stmt, index, boolValue ? 1 : 0);
            }
            else if (value is uint or long or ulong) {
                SQLiteInterop.BindInt64(stmt, index, Convert.ToInt64(value));
            }
            else if (value is float or double or decimal) {
                SQLiteInterop.BindDouble(stmt, index, Convert.ToDouble(value));
            }
            else if (value is TimeSpan timeSpanValue) {
                SQLiteInterop.BindInt64(stmt, index, timeSpanValue.Ticks);
            }
            else if (value is DateTime dateTimeValue) {
                SQLiteInterop.BindInt64(stmt, index, dateTimeValue.Ticks);
            }
            else if (value is DateTimeOffset dateTimeOffsetValue) {
                SQLiteInterop.BindBlob(stmt, index, DateTimeOffsetToBytes(dateTimeOffsetValue));
            }
            else if (value is byte[] byteArrayValue) {
                SQLiteInterop.BindBlob(stmt, index, byteArrayValue);
            }
            else if (value is Guid or StringBuilder or Uri or UriBuilder) {
                SQLiteInterop.BindText(stmt, index, value.ToString()!);
            }
            else {
                // Now we could possibly get an enum, retrieve cached info
                Type valueType = value.GetType();
                if (valueType.IsEnum) {
                    if (valueType.GetCustomAttribute<StoreByNameAttribute>() is not null) {
                        SQLiteInterop.BindText(stmt, index, Enum.GetName(valueType, value)!);
                    }
                    else {
                        SQLiteInterop.BindInt(stmt, index, Convert.ToInt32(value));
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

    private static object? ReadCol(Sqlite3Statement stmt, int index, SQLiteInterop.ColType type, Type clrType) {
        if (type is SQLiteInterop.ColType.Null) {
            return null;
        }
        else {
            if (Nullable.GetUnderlyingType(clrType) is Type underlyingType) {
                clrType = underlyingType;
            }

            if (clrType == typeof(string)) {
                return SQLiteInterop.ColumnString(stmt, index);
            }
            else if (clrType == typeof(int)) {
                return (int)SQLiteInterop.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(bool)) {
                return SQLiteInterop.ColumnInt(stmt, index) == 1;
            }
            else if (clrType == typeof(double)) {
                return SQLiteInterop.ColumnDouble(stmt, index);
            }
            else if (clrType == typeof(float)) {
                return (float)SQLiteInterop.ColumnDouble(stmt, index);
            }
            else if (clrType == typeof(TimeSpan)) {
                return new TimeSpan(SQLiteInterop.ColumnInt64(stmt, index));
            }
            else if (clrType == typeof(DateTime)) {
                return new DateTime(SQLiteInterop.ColumnInt64(stmt, index));
            }
            else if (clrType == typeof(DateTimeOffset)) {
                return BytesToDateTimeOffset(SQLiteInterop.ColumnBlob(stmt, index));
            }
            else if (clrType.IsEnum) {
                if (type is SQLiteInterop.ColType.Text) {
                    string value = SQLiteInterop.ColumnString(stmt, index);
                    return Enum.Parse(clrType, value, true);
                }
                else {
                    return SQLiteInterop.ColumnInt(stmt, index);
                }
            }
            else if (clrType == typeof(long)) {
                return SQLiteInterop.ColumnInt64(stmt, index);
            }
            else if (clrType == typeof(ulong)) {
                return (ulong)SQLiteInterop.ColumnInt64(stmt, index);
            }
            else if (clrType == typeof(uint)) {
                return (uint)SQLiteInterop.ColumnInt64(stmt, index);
            }
            else if (clrType == typeof(decimal)) {
                return (decimal)SQLiteInterop.ColumnDouble(stmt, index);
            }
            else if (clrType == typeof(byte)) {
                return (byte)SQLiteInterop.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(ushort)) {
                return (ushort)SQLiteInterop.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(short)) {
                return (short)SQLiteInterop.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(sbyte)) {
                return (sbyte)SQLiteInterop.ColumnInt(stmt, index);
            }
            else if (clrType == typeof(byte[])) {
                return SQLiteInterop.ColumnBlob(stmt, index);
            }
            else if (clrType == typeof(Guid)) {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new Guid(text);
            }
            else if (clrType == typeof(Uri)) {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new Uri(text);
            }
            else if (clrType == typeof(StringBuilder)) {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new StringBuilder(text);
            }
            else if (clrType == typeof(UriBuilder)) {
                string text = SQLiteInterop.ColumnString(stmt, index);
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

internal class FastColumnSetter {
    /// <summary>
    /// Creates a delegate that can be used to quickly set object members from query columns.
    ///
    /// Note that this frontloads the slow reflection-based type checking for columns to only happen once at the beginning of a query,
    /// and then afterwards each row of the query can invoke the delegate returned by this function to get much better performance (up to 10x speed boost, depending on query size and platform).
    /// </summary>
    /// <typeparam name="T">The type of the destination object that the query will read into</typeparam>
    /// <param name="conn">The active connection.  Note that this is primarily needed in order to read preferences regarding how certain data types (such as TimeSpan / DateTime) should be encoded in the database.</param>
    /// <param name="column">The table mapping used to map the statement column to a member of the destination object type</param>
    /// <returns>
    /// A delegate for fast-setting of object members from statement columns.
    ///
    /// If no fast setter is available for the requested column (enums in particular cause headache), then this function returns null.
    /// </returns>
    internal static Action<object, Sqlite3Statement, int>? GetFastSetter<T>(TableMapping.Column column) {
        Type clrType = column.PropertyInfo!.PropertyType;

        if (Nullable.GetUnderlyingType(clrType) is Type underlyingType) {
            clrType = underlyingType;
        }

        if (clrType == typeof(string)) {
            return CreateTypedSetterDelegate<T, string>(column, (stmt, index) => {
                return SQLiteInterop.ColumnString(stmt, index);
            });
        }
        else if (clrType == typeof(int)) {
            return CreateNullableTypedSetterDelegate<T, int>(column, (stmt, index) => {
                return SQLiteInterop.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(bool)) {
            return CreateNullableTypedSetterDelegate<T, bool>(column, (stmt, index) => {
                return SQLiteInterop.ColumnInt(stmt, index) == 1;
            });
        }
        else if (clrType == typeof(double)) {
            return CreateNullableTypedSetterDelegate<T, double>(column, (stmt, index) => {
                return SQLiteInterop.ColumnDouble(stmt, index);
            });
        }
        else if (clrType == typeof(float)) {
            return CreateNullableTypedSetterDelegate<T, float>(column, (stmt, index) => {
                return (float)SQLiteInterop.ColumnDouble(stmt, index);
            });
        }
        else if (clrType == typeof(TimeSpan)) {
            return CreateNullableTypedSetterDelegate<T, TimeSpan>(column, (stmt, index) => {
                return new TimeSpan(SQLiteInterop.ColumnInt64(stmt, index));
            });
        }
        else if (clrType == typeof(DateTime)) {
            return CreateNullableTypedSetterDelegate<T, DateTime>(column, (stmt, index) => {
                return new DateTime(SQLiteInterop.ColumnInt64(stmt, index));
            });
        }
        else if (clrType == typeof(DateTimeOffset)) {
            return CreateNullableTypedSetterDelegate<T, DateTimeOffset>(column, (stmt, index) => {
                return SQLiteCommand.BytesToDateTimeOffset(SQLiteInterop.ColumnBlob(stmt, index));
            });
        }
        else if (clrType.IsEnum) {
            // NOTE: Not sure of a good way (if any?) to do a strongly-typed fast setter like this for enumerated types -- for now, return null and column sets will revert back to the safe (but slow) Reflection-based method of column prop.Set()
        }
        else if (clrType == typeof(long)) {
            return CreateNullableTypedSetterDelegate<T, long>(column, (stmt, index) => {
                return SQLiteInterop.ColumnInt64(stmt, index);
            });
        }
        else if (clrType == typeof(ulong)) {
            return CreateNullableTypedSetterDelegate<T, ulong>(column, (stmt, index) => {
                return (ulong)SQLiteInterop.ColumnInt64(stmt, index);
            });
        }
        else if (clrType == typeof(uint)) {
            return CreateNullableTypedSetterDelegate<T, uint>(column, (stmt, index) => {
                return (uint)SQLiteInterop.ColumnInt64(stmt, index);
            });
        }
        else if (clrType == typeof(decimal)) {
            return CreateNullableTypedSetterDelegate<T, decimal>(column, (stmt, index) => {
                return (decimal)SQLiteInterop.ColumnDouble(stmt, index);
            });
        }
        else if (clrType == typeof(byte)) {
            return CreateNullableTypedSetterDelegate<T, byte>(column, (stmt, index) => {
                return (byte)SQLiteInterop.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(ushort)) {
            return CreateNullableTypedSetterDelegate<T, ushort>(column, (stmt, index) => {
                return (ushort)SQLiteInterop.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(short)) {
            return CreateNullableTypedSetterDelegate<T, short>(column, (stmt, index) => {
                return (short)SQLiteInterop.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(sbyte)) {
            return CreateNullableTypedSetterDelegate<T, sbyte>(column, (stmt, index) => {
                return (sbyte)SQLiteInterop.ColumnInt(stmt, index);
            });
        }
        else if (clrType == typeof(byte[])) {
            return CreateTypedSetterDelegate<T, byte[]>(column, (stmt, index) => {
                return SQLiteInterop.ColumnBlob(stmt, index);
            });
        }
        else if (clrType == typeof(Guid)) {
            return CreateNullableTypedSetterDelegate<T, Guid>(column, (stmt, index) => {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new Guid(text);
            });
        }
        else if (clrType == typeof(StringBuilder)) {
            return CreateTypedSetterDelegate<T, StringBuilder>(column, (stmt, index) => {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new StringBuilder(text);
            });
        }
        else if (clrType == typeof(Uri)) {
            return CreateTypedSetterDelegate<T, Uri>(column, (stmt, index) => {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new Uri(text);
            });
        }
        else if (clrType == typeof(UriBuilder)) {
            return CreateTypedSetterDelegate<T, UriBuilder>(column, (stmt, index) => {
                string text = SQLiteInterop.ColumnString(stmt, index);
                return new UriBuilder(text);
            });
        }
        else {
            // NOTE: Will fall back to the slow setter method in the event that we are unable to create a fast setter delegate for a particular column type
        }
        return null;
    }

    /// <summary>
    /// This creates a strongly typed delegate that will permit fast setting of column values given a Sqlite3Statement and a column index.
    ///
    /// Note that this is identical to CreateTypedSetterDelegate(), but has an extra check to see if it should create a nullable version of the delegate.
    /// </summary>
    /// <typeparam name="ObjectType">The type of the object whose member column is being set</typeparam>
    /// <typeparam name="ColumnMemberType">The CLR type of the member in the object which corresponds to the given SQLite columnn</typeparam>
    /// <param name="column">The column mapping that identifies the target member of the destination object</param>
    /// <param name="getColumnValue">A lambda that can be used to retrieve the column value at query-time</param>
    /// <returns>A strongly-typed delegate</returns>
    private static Action<object, Sqlite3Statement, int> CreateNullableTypedSetterDelegate<ObjectType, ColumnMemberType>(TableMapping.Column column, Func<Sqlite3Statement, int, ColumnMemberType> getColumnValue) where ColumnMemberType : struct {
        Type clrType = column.PropertyInfo!.PropertyType;
        bool isNullable = false;

        if (Nullable.GetUnderlyingType(clrType) is not null) {
            isNullable = true;
        }

        if (isNullable) {
            Action<ObjectType, ColumnMemberType?> setProperty = (Action<ObjectType, ColumnMemberType?>)Delegate.CreateDelegate(
                typeof(Action<ObjectType, ColumnMemberType?>),
                null,
                column.PropertyInfo.GetSetMethod()!
            );

            return (obj, stmt, i) => {
                SQLiteInterop.ColType colType = SQLiteInterop.ColumnType(stmt, i);
                if (colType is not SQLiteInterop.ColType.Null) {
                    setProperty.Invoke((ObjectType)obj, getColumnValue.Invoke(stmt, i));
                }
            };
        }

        return CreateTypedSetterDelegate<ObjectType, ColumnMemberType>(column, getColumnValue);
    }

    /// <summary>
    /// This creates a strongly typed delegate that will permit fast setting of column values given a Sqlite3Statement and a column index.
    /// </summary>
    /// <typeparam name="ObjectType">The type of the object whose member column is being set</typeparam>
    /// <typeparam name="ColumnMemberType">The CLR type of the member in the object which corresponds to the given SQLite columnn</typeparam>
    /// <param name="column">The column mapping that identifies the target member of the destination object</param>
    /// <param name="getColumnValue">A lambda that can be used to retrieve the column value at query-time</param>
    /// <returns>A strongly-typed delegate</returns>
    private static Action<object, Sqlite3Statement, int> CreateTypedSetterDelegate<ObjectType, ColumnMemberType>(TableMapping.Column column, Func<Sqlite3Statement, int, ColumnMemberType> getColumnValue) {
        Action<ObjectType, ColumnMemberType> setProperty = (Action<ObjectType, ColumnMemberType>)Delegate.CreateDelegate(
            typeof(Action<ObjectType, ColumnMemberType>),
            null,
            column.PropertyInfo!.GetSetMethod()!
        );

        return (obj, stmt, i) => {
            SQLiteInterop.ColType colType = SQLiteInterop.ColumnType(stmt, i);
            if (colType != SQLiteInterop.ColType.Null) {
                setProperty.Invoke((ObjectType)obj, getColumnValue.Invoke(stmt, i));
            }
        };
    }
}

public abstract class BaseTableQuery {
    protected class Ordering(string columnName, bool ascending) {
        public string ColumnName { get; } = columnName;
        public bool Ascending { get; } = ascending;
    }
}

public class TableQuery<T> : BaseTableQuery, IEnumerable<T> {
    public SQLiteConnection Connection { get; }
    public TableMapping Table { get; }

    private Expression? _where;
    private List<Ordering>? _orderBys;
    private int? _limit;
    private int? _offset;

    private BaseTableQuery? _joinInner;
    private Expression? _joinInnerKeySelector;
    private BaseTableQuery? _joinOuter;
    private Expression? _joinOuterKeySelector;
    private Expression? _joinSelector;

    private Expression? _selector;

    private TableQuery(SQLiteConnection connection, TableMapping table) {
        Connection = connection;
        Table = table;
    }
    public TableQuery(SQLiteConnection connection) {
        Connection = connection;
        Table = Connection.GetMapping<T>();
    }

    public TableQuery<U> Clone<U>() {
        TableQuery<U> query = new(Connection, Table) {
            _where = _where,
            _orderBys = _orderBys?.ToList(),
            _limit = _limit,
            _offset = _offset,
            _joinInner = _joinInner,
            _joinInnerKeySelector = _joinInnerKeySelector,
            _joinOuter = _joinOuter,
            _joinOuterKeySelector = _joinOuterKeySelector,
            _joinSelector = _joinSelector,
            _selector = _selector,
        };
        return query;
    }

    /// <summary>
    /// Filters the query based on a predicate.
    /// </summary>
    public TableQuery<T> Where(Expression<Func<T, bool>> predicateExpression) {
        if (predicateExpression.NodeType is ExpressionType.Lambda) {
            LambdaExpression lambda = predicateExpression;
            Expression pred = lambda.Body;
            TableQuery<T> query = Clone<T>();
            query.AddWhere(pred);
            return query;
        }
        else {
            throw new NotSupportedException("Must be a predicate");
        }
    }

    /// <summary>
    /// Delete all the rows that match this query.
    /// </summary>
    public int Delete() {
        return Delete(null);
    }
    /// <summary>
    /// Delete all the rows that match this query and the given predicate.
    /// </summary>
    public int Delete(Expression<Func<T, bool>>? predicateExpression) {
        if (_limit is not null || _offset is not null) {
            throw new InvalidOperationException("Cannot delete with limits or offsets");
        }
        if (_where is null && predicateExpression is null) {
            throw new InvalidOperationException("No condition specified");
        }

        Expression? predicate = _where;
        if (predicateExpression is not null && predicateExpression.NodeType is ExpressionType.Lambda) {
            LambdaExpression lambda = predicateExpression;
            predicate = predicate is not null ? Expression.AndAlso(predicate, lambda.Body) : lambda.Body;
        }

        List<object?> parameters = [];
        string commandText = $"delete from \"{Table.TableName}\" where {CompileExpression(predicate!, parameters).CommandText}";
        SQLiteCommand command = Connection.CreateCommand(commandText, parameters);

        int rowCount = command.ExecuteNonQuery();
        return rowCount;
    }

    /// <summary>
    /// Yields a given number of elements from the query and then skips the remainder.
    /// </summary>
    public TableQuery<T> Take(int n) {
        TableQuery<T> query = Clone<T>();
        query._limit = n;
        return query;
    }
    /// <summary>
    /// Skips a given number of elements from the query and then yields the remainder.
    /// </summary>
    public TableQuery<T> Skip(int n) {
        TableQuery<T> query = Clone<T>();
        query._offset = n;
        return query;
    }

    /// <summary>
    /// Returns the element at a given index.
    /// </summary>
    public T ElementAt(int index) {
        return Skip(index).Take(1).First();
    }

    /// <summary>
    /// Orders the query results according to a key.
    /// </summary>
    public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> expression) {
        return AddOrderBy(expression, true);
    }
    /// <summary>
    /// Orders the query results according to a key.
    /// </summary>
    public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> expression) {
        return AddOrderBy(expression, false);
    }

    private TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpression, bool ascending) {
        LambdaExpression lambdaExpression = orderExpression;

        MemberExpression? memberExpression;
        if (lambdaExpression.Body is UnaryExpression unary && unary.NodeType is ExpressionType.Convert) {
            memberExpression = unary.Operand as MemberExpression;
        }
        else {
            memberExpression = lambdaExpression.Body as MemberExpression;
        }

        if (memberExpression is not null && memberExpression.Expression?.NodeType is ExpressionType.Parameter) {
            TableQuery<T> query = Clone<T>();
            query._orderBys ??= [];
            query._orderBys.Add(new Ordering(Table.FindColumnWithPropertyName(memberExpression.Member.Name)!.Name, ascending));
            return query;
        }
        else {
            throw new NotSupportedException($"Order By does not support: {orderExpression}");
        }
    }

    private void AddWhere(Expression pred) {
        if (_where is null) {
            _where = pred;
        }
        else {
            _where = Expression.AndAlso(_where, pred);
        }
    }

    private SQLiteCommand GenerateCommand(string selectionList) {
        if (_joinInner is not null && _joinOuter is not null) {
            throw new NotSupportedException("Joins are not supported.");
        }

        string commandText = $"select {selectionList} from \"{Table.TableName}\"";
        List<object?> parameters = [];
        if (_where is not null) {
            commandText += $" where {CompileExpression(_where, parameters).CommandText}";
        }
        if ((_orderBys is not null) && (_orderBys.Count > 0)) {
            string orderByString = string.Join(", ", _orderBys.Select(orderBy => $"\"{orderBy.ColumnName}\"" + (orderBy.Ascending ? "" : " desc")));
            commandText += $" order by {orderByString}";
        }
        if (_limit is not null) {
            commandText += $" limit {_limit.Value}";
        }
        if (_offset.HasValue) {
            if (_limit is null) {
                commandText += " limit -1 ";
            }
            commandText += $" offset {_offset.Value}";
        }
        return Connection.CreateCommand(commandText, parameters);
    }

    private class CompileResult {
        public string? CommandText { get; set; }
        public object? Value { get; set; }
    }

    private CompileResult CompileExpression(Expression expression, List<object?> queryParameters) {
        if (expression is null) {
            throw new NotSupportedException("Expression is NULL");
        }
        else if (expression is BinaryExpression binaryExpression) {
            // VB turns 'x=="foo"' into 'CompareString(x,"foo",true/false)==0', so we need to unwrap it
            // http://blogs.msdn.com/b/vbteam/archive/2007/09/18/vb-expression-trees-string-comparisons.aspx
            if (binaryExpression.Left.NodeType is ExpressionType.Call) {
                MethodCallExpression call = (MethodCallExpression)binaryExpression.Left;
                if (call.Method.DeclaringType!.FullName == "Microsoft.VisualBasic.CompilerServices.Operators" && call.Method.Name == "CompareString") {
                    binaryExpression = Expression.MakeBinary(binaryExpression.NodeType, call.Arguments[0], call.Arguments[1]);
                }
            }

            CompileResult leftResult = CompileExpression(binaryExpression.Left, queryParameters);
            CompileResult rightResult = CompileExpression(binaryExpression.Right, queryParameters);

            // If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
            string text;
            if (leftResult.CommandText == "?" && leftResult.Value == null) {
                text = CompileNullBinaryExpression(binaryExpression, rightResult);
            }
            else if (rightResult.CommandText == "?" && rightResult.Value == null) {
                text = CompileNullBinaryExpression(binaryExpression, leftResult);
            }
            else {
                text = "(" + leftResult.CommandText + " " + GetSqlOperator(binaryExpression.NodeType) + " " + rightResult.CommandText + ")";
            }
            return new CompileResult() {
                CommandText = text
            };
        }
        else if (expression.NodeType is ExpressionType.Not) {
            Expression operandExpression = ((UnaryExpression)expression).Operand;
            CompileResult operand = CompileExpression(operandExpression, queryParameters);
            object? value = operand.Value;
            if (value is bool boolValue) {
                value = !boolValue;
            }
            return new CompileResult() {
                CommandText = "NOT(" + operand.CommandText + ")",
                Value = value
            };
        }
        else if (expression.NodeType is ExpressionType.Call) {
            MethodCallExpression call = (MethodCallExpression)expression;
            CompileResult[] callArguments = new CompileResult[call.Arguments.Count];
            CompileResult? callTarget = call.Object is not null ? CompileExpression(call.Object, queryParameters) : null;

            for (int i = 0; i < callArguments.Length; i++) {
                callArguments[i] = CompileExpression(call.Arguments[i], queryParameters);
            }

            string sqlCall = "";

            if (call.Method.Name is "Like" && callArguments.Length == 2) {
                sqlCall = "(" + callArguments[0].CommandText + " like " + callArguments[1].CommandText + ")";
            }
            else if (call.Method.Name is "Contains" && callArguments.Length == 2) {
                sqlCall = "(" + callArguments[1].CommandText + " in " + callArguments[0].CommandText + ")";
            }
            else if (call.Method.Name is "Contains" && callArguments.Length == 1) {
                if (call.Object != null && call.Object.Type == typeof(string)) {
                    sqlCall = "( instr(" + callTarget!.CommandText + "," + callArguments[0].CommandText + ") >0 )";
                }
                else {
                    sqlCall = "(" + callArguments[0].CommandText + " in " + callTarget!.CommandText + ")";
                }
            }
            else if (call.Method.Name is "StartsWith" && callArguments.Length >= 1) {
                StringComparison comparisonType = StringComparison.CurrentCulture;
                if (callArguments.Length == 2) {
                    comparisonType = (StringComparison)callArguments[1].Value!;
                }
                switch (comparisonType) {
                    case StringComparison.Ordinal or StringComparison.CurrentCulture:
                        sqlCall = "( substr(" + callTarget!.CommandText + ", 1, " + callArguments[0].Value!.ToString()!.Length + ") =  " + callArguments[0].CommandText + ")";
                        break;
                    case StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase:
                        sqlCall = "(" + callTarget!.CommandText + " like (" + callArguments[0].CommandText + " || '%'))";
                        break;
                }
            }
            else if (call.Method.Name is "EndsWith" && callArguments.Length >= 1) {
                StringComparison comparisonType = StringComparison.CurrentCulture;
                if (callArguments.Length == 2) {
                    comparisonType = (StringComparison)callArguments[1].Value!;
                }
                switch (comparisonType) {
                    case StringComparison.Ordinal or StringComparison.CurrentCulture:
                        sqlCall = "( substr(" + callTarget!.CommandText + ", length(" + callTarget.CommandText + ") - " + callArguments[0].Value!.ToString()!.Length + "+1, " + callArguments[0].Value!.ToString()!.Length + ") =  " + callArguments[0].CommandText + ")";
                        break;
                    case StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase:
                        sqlCall = "(" + callTarget!.CommandText + " like ('%' || " + callArguments[0].CommandText + "))";
                        break;
                }
            }
            else if (call.Method.Name is "Equals" && callArguments.Length == 1) {
                sqlCall = "(" + callTarget!.CommandText + " = (" + callArguments[0].CommandText + "))";
            }
            else if (call.Method.Name is "ToLower") {
                sqlCall = "(lower(" + callTarget!.CommandText + "))";
            }
            else if (call.Method.Name is "ToUpper") {
                sqlCall = "(upper(" + callTarget!.CommandText + "))";
            }
            else if (call.Method.Name is "Replace" && callArguments.Length == 2) {
                sqlCall = "(replace(" + callTarget!.CommandText + "," + callArguments[0].CommandText + "," + callArguments[1].CommandText + "))";
            }
            else if (call.Method.Name is "IsNullOrEmpty" && callArguments.Length == 1) {
                sqlCall = "(" + callArguments[0].CommandText + " is null or" + callArguments[0].CommandText + " ='' )";
            }
            else {
                sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", callArguments.Select(a => a.CommandText).ToArray()) + ")";
            }

            return new CompileResult() {
                CommandText = sqlCall
            };

        }
        else if (expression.NodeType is ExpressionType.Constant) {
            ConstantExpression constantExpression = (ConstantExpression)expression;
            queryParameters.Add(constantExpression.Value);
            return new CompileResult() {
                CommandText = "?",
                Value = constantExpression.Value
            };
        }
        else if (expression.NodeType is ExpressionType.Convert) {
            UnaryExpression unaryExpression = (UnaryExpression)expression;
            CompileResult valueResult = CompileExpression(unaryExpression.Operand, queryParameters);
            return new CompileResult {
                CommandText = valueResult.CommandText,
                Value = valueResult.Value is not null ? ConvertTo(valueResult.Value, unaryExpression.Type) : null
            };
        }
        else if (expression.NodeType is ExpressionType.MemberAccess) {
            MemberExpression memberExpression = (MemberExpression)expression;

            ParameterExpression? parameterExpression = memberExpression.Expression as ParameterExpression;
            if (parameterExpression is null) {
                if (memberExpression.Expression is UnaryExpression convert && convert.NodeType == ExpressionType.Convert) {
                    parameterExpression = convert.Operand as ParameterExpression;
                }
            }

            if (parameterExpression is not null) {
                // This is a column of our table, output just the column name
                // Need to translate it if that column name is mapped
                string columnName = Table.FindColumnWithPropertyName(memberExpression.Member.Name)!.Name;
                return new CompileResult() {
                    CommandText = $"\"{columnName}\""
                };
            }
            else {
                object? memberTarget = null;
                if (memberExpression.Expression != null) {
                    CompileResult result = CompileExpression(memberExpression.Expression, queryParameters);
                    if (result.Value is null) {
                        throw new NotSupportedException("Member access failed to compile expression");
                    }
                    if (result.CommandText is "?") {
                        queryParameters.RemoveAt(queryParameters.Count - 1);
                    }
                    memberTarget = result.Value;
                }

                // Get the member value
                object? memberValue = memberExpression.Member switch {
                    PropertyInfo propertyInfo => propertyInfo.GetValue(memberTarget),
                    FieldInfo fieldInfo => fieldInfo.GetValue(memberTarget),
                    _ => throw new NotSupportedException($"MemberExpression: {memberExpression.Member.GetType()}")
                };

                // Work special magic for enumerables
                if (memberValue is IEnumerable and not (string or IEnumerable<byte>)) {
                    StringBuilder builder = new();
                    builder.Append('(');
                    string comma = "";
                    foreach (object item in (IEnumerable)memberValue) {
                        queryParameters.Add(item);
                        builder.Append(comma);
                        builder.Append('?');
                        comma = ",";
                    }
                    builder.Append(')');
                    return new CompileResult() {
                        CommandText = builder.ToString(),
                        Value = memberValue
                    };
                }
                else {
                    queryParameters.Add(memberValue);
                    return new CompileResult() {
                        CommandText = "?",
                        Value = memberValue
                    };
                }
            }
        }
        throw new NotSupportedException($"Cannot compile: {expression.NodeType}");
    }

    private static object? ConvertTo(object? obj, Type type) {
        if (Nullable.GetUnderlyingType(type) is Type underlyingType) {
            if (obj is null) {
                return null;
            }
            return Convert.ChangeType(obj, underlyingType);
        }
        else {
            return Convert.ChangeType(obj, type);
        }
    }

    /// <summary>
    /// Compiles a BinaryExpression where one of the parameters is null.
    /// </summary>
    /// <param name="expression">The expression to compile</param>
    /// <param name="parameter">The non-null parameter</param>
    private static string CompileNullBinaryExpression(BinaryExpression expression, CompileResult parameter) {
        if (expression.NodeType is ExpressionType.Equal) {
            return $"({parameter.CommandText} is ?)";
        }
        else if (expression.NodeType is ExpressionType.NotEqual) {
            return $"({parameter.CommandText} is not ?)";
        }
        else if (expression.NodeType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual) {
            return $"({parameter.CommandText} < ?)"; // always false
        }
        else {
            throw new NotSupportedException($"Cannot compile Null-BinaryExpression with type {expression.NodeType}");
        }
    }
    private static string GetSqlOperator(ExpressionType expressionType) {
        return expressionType switch {
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.And => "&",
            ExpressionType.AndAlso => "and",
            ExpressionType.Or => "|",
            ExpressionType.OrElse => "or",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            _ => throw new NotSupportedException($"Cannot get SQL operator for {expressionType}")
        };
    }

    /// <summary>
    /// Returns the number of elements matching the query.
    /// </summary>
    public int Count() {
        return GenerateCommand("count(*)").ExecuteScalar<int>();
    }
    /// <inheritdoc cref="Count()"/>
    public long LongCount() {
        return GenerateCommand("count(*)").ExecuteScalar<long>();
    }
    /// <summary>
    /// Returns the number of elements matching the query and the predicate.
    /// </summary>
    public int Count(Expression<Func<T, bool>> predicate) {
        return Where(predicate).Count();
    }
    /// <inheritdoc cref="Count(Expression{Func{T, bool}})"/>
    public long LongCount(Expression<Func<T, bool>> predicate) {
        return Where(predicate).LongCount();
    }

    /// <summary>
    /// Returns an enumerator that iterates through the elements matching the query.
    /// </summary>
    public IEnumerator<T> GetEnumerator() {
        return GenerateCommand("*").ExecuteQuery<T>().GetEnumerator();
    }
    /// <inheritdoc cref="GetEnumerator()"/>
    IEnumerator IEnumerable.GetEnumerator() {
        return GetEnumerator();
    }

    /// <summary>
    /// Returns a list of all the elements matching the query.
    /// </summary>
    public List<T> ToList() {
        return GenerateCommand("*").ExecuteQuery<T>().ToList();
    }
    /// <summary>
    /// Returns an array of all the elements matching the query.
    /// </summary>
    public T[] ToArray() {
        return GenerateCommand("*").ExecuteQuery<T>().ToArray();
    }
    /// <summary>
    /// Returns the first element matching the query, or throws.
    /// </summary>
    public T First() {
        return GenerateCommand("*").ExecuteQuery<T>().First();
    }
    /// <summary>
    /// Returns the first element matching the query and the predicate, or throws.
    /// </summary>
    public T First(Expression<Func<T, bool>> predicate) {
        return Where(predicate).First();
    }
    /// <summary>
    /// Returns the first element matching the query, or <see langword="null"/> if no element is found.
    /// </summary>
    public T? FirstOrDefault() {
        return GenerateCommand("*").ExecuteQuery<T>().FirstOrDefault();
    }
    /// <summary>
    /// Returns the first element matching the query and the predicate, or <see langword="null"/> if no element is found.
    /// </summary>
    public T? FirstOrDefault(Expression<Func<T, bool>> predicate) {
        return Where(predicate).FirstOrDefault();
    }
}