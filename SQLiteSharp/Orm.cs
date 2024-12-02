using System.Collections.Concurrent;
using System.Text;
using System.Reflection;

namespace SQLiteSharp;

public class Orm {
    public ConcurrentDictionary<Type, TableMap> TableMaps { get; } = [];
    public ConcurrentDictionary<Type, TypeSerializer> TypeSerializers { get; } = [];
    public TypeSerializer? TypeSerializerFallback { get; set; } = null;
    public Func<MemberInfo, bool> IsImplicitPrimaryKey { get; set; } = Member => Member.Name == "Id";
    public Func<MemberInfo, bool> IsImplicitIndex { get; set; } = Member => Member.Name.EndsWith("Id");
    public Func<MemberInfo, bool> IsImplicitAutoIncrementedPrimaryKey { get; set; } = Member => false;

    public static Orm Default { get; } = new();

    public Orm() {
        AddDefaultTypeSerializers();
    }
    public void RegisterType(Type type, SqliteType sqliteType, Func<object, SqliteValue> serialize, Func<SqliteValue, object?> deserialize) {
        TypeSerializers[type] = new TypeSerializer(type, sqliteType, serialize, deserialize);
    }
    public void RegisterType<T>(SqliteType sqliteType, Func<T, SqliteValue> serialize, Func<SqliteValue, object> deserialize) {
        RegisterType(typeof(T), sqliteType, (object clr) => serialize((T)clr), (SqliteValue sqlite) => deserialize(sqlite));
    }
    public bool UnregisterType(Type type) {
        return TypeSerializers.TryRemove(type, out _);
    }
    public bool UnregisterType<T>() {
        return TypeSerializers.TryRemove(typeof(T), out _);
    }
    public TypeSerializer GetTypeSerializer(Type type) {
        // Get non-nullable type (int? -> int)
        type = AsUnderlyingType(type);
        // Try get serializer for exact type
        if (TypeSerializers.TryGetValue(type, out TypeSerializer mapper)) {
            return mapper;
        }
        // Try get fallback serializer
        if (TypeSerializerFallback is not null) {
            return TypeSerializerFallback.Value;
        }
        // Serializer not found
        throw new InvalidOperationException($"No {nameof(TypeSerializer)} found for '{type}'");
    }
    public object? ReadColumn(Sqlite3Statement statement, int index, Type type) {
        TypeSerializer typeSerializer = GetTypeSerializer(type);
        SqliteValue value = SQLiteRaw.GetColumnValue(statement, index);
        return typeSerializer.Deserialize(value);
    }
    public void BindParameter(Sqlite3Statement statement, int index, object? value) {
        if (value is null) {
            SQLiteRaw.BindNull(statement, index);
            return;
        }

        TypeSerializer typeSerializer = GetTypeSerializer(value.GetType());
        SqliteValue rawValue = typeSerializer.Serialize(value);

        switch (rawValue.SqliteType) {
            case SqliteType.Null:
                SQLiteRaw.BindNull(statement, index);
                break;
            case SqliteType.Integer:
                SQLiteRaw.BindInt64(statement, index, rawValue.AsInteger);
                break;
            case SqliteType.Float:
                SQLiteRaw.BindDouble(statement, index, rawValue.AsFloat);
                break;
            case SqliteType.Text:
                SQLiteRaw.BindText(statement, index, rawValue.AsText);
                break;
            case SqliteType.Blob:
                SQLiteRaw.BindBlob(statement, index, rawValue.AsBlob);
                break;
            default:
                throw new NotImplementedException($"Cannot bind column type '{rawValue.SqliteType}'");
        }
    }
    public string GetSqlDeclaration(ColumnMap column) {
        TypeSerializer typeSerializer = GetTypeSerializer(column.ClrType);

        string declaration = $"{Quote(column.Name)} {Quote(GetTypeSql(typeSerializer.SqliteType))} collate {Quote(column.Collation)} ";

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
    public static bool IsPrimaryKey(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<PrimaryKeyAttribute>() is not null;
    }
    public static bool IsAutoIncrement(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<AutoIncrementAttribute>() is not null;
    }
    public static bool IsNotNullConstrained(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<NotNullAttribute>() is not null;
    }
    public static string GetCollation(MemberInfo memberInfo) {
		return memberInfo.GetCustomAttribute<CollationAttribute>()?.Value ?? CollationType.Binary;
    }
    public static IEnumerable<IndexedAttribute> GetIndexes(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttributes<IndexedAttribute>();
    }
    public static int? GetMaxStringLength(MemberInfo memberInfo) {
        return memberInfo.GetCustomAttribute<MaxLengthAttribute>()?.Value;
    }
    public static Type AsUnderlyingType(Type Type) {
        return Nullable.GetUnderlyingType(Type) ?? Type;
    }

    private void AddDefaultTypeSerializers() {
        RegisterType<bool>(
            sqliteType: SqliteType.Integer,
            serialize: (bool clr) => clr ? 1 : 0,
            deserialize: (SqliteValue sqlite) => (int)sqlite.AsInteger != 0
        );
        RegisterType<string>(
            sqliteType: SqliteType.Text,
            serialize: (string clr) => clr,
            deserialize: (SqliteValue sqlite) => sqlite.AsText
        );
        RegisterType<byte>(
            sqliteType: SqliteType.Integer,
            serialize: (byte clr) => clr,
            deserialize: (SqliteValue sqlite) => (byte)sqlite.AsInteger
        );
        RegisterType<sbyte>(
            sqliteType: SqliteType.Integer,
            serialize: (sbyte clr) => clr,
            deserialize: (SqliteValue sqlite) => (sbyte)sqlite.AsInteger
        );
        RegisterType<short>(
            sqliteType: SqliteType.Integer,
            serialize: (short clr) => clr,
            deserialize: (SqliteValue sqlite) => (short)sqlite.AsInteger
        );
        RegisterType<ushort>(
            sqliteType: SqliteType.Integer,
            serialize: (ushort clr) => clr,
            deserialize: (SqliteValue sqlite) => (ushort)sqlite.AsInteger
        );
        RegisterType<int>(
            sqliteType: SqliteType.Integer,
            serialize: (int clr) => clr,
            deserialize: (SqliteValue sqlite) => (int)sqlite.AsInteger
        );
        RegisterType<uint>(
            sqliteType: SqliteType.Integer,
            serialize: (uint clr) => clr,
            deserialize: (SqliteValue sqlite) => (uint)sqlite.AsInteger
        );
        RegisterType<long>(
            sqliteType: SqliteType.Integer,
            serialize: (long clr) => clr,
            deserialize: (SqliteValue sqlite) => sqlite.AsInteger
        );
        RegisterType<ulong>(
            sqliteType: SqliteType.Integer,
            serialize: (ulong clr) => clr,
            deserialize: (SqliteValue sqlite) => (ulong)sqlite.AsInteger
        );
        RegisterType<char>(
            sqliteType: SqliteType.Integer,
            serialize: (char clr) => clr,
            deserialize: (SqliteValue sqlite) => sqlite.AsInteger
        );
        RegisterType<float>(
            sqliteType: SqliteType.Integer,
            serialize: (float clr) => clr,
            deserialize: (SqliteValue sqlite) => (float)sqlite.AsFloat
        );
        RegisterType<double>(
            sqliteType: SqliteType.Integer,
            serialize: (double clr) => clr,
            deserialize: (SqliteValue sqlite) => sqlite.AsFloat
        );
        RegisterType<TimeSpan>(
            sqliteType: SqliteType.Integer,
            serialize: (TimeSpan clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite) => new TimeSpan(sqlite.AsInteger)
        );
        RegisterType<DateTime>(
            sqliteType: SqliteType.Integer,
            serialize: (DateTime clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite) => new DateTime(sqlite.AsInteger)
        );
        RegisterType<Uri>(
            sqliteType: SqliteType.Text,
            serialize: (Uri clr) => clr.AbsoluteUri,
            deserialize: (SqliteValue sqlite) => new Uri(sqlite.AsText)
        );
        RegisterType<byte[]>(
            sqliteType: SqliteType.Blob,
            serialize: (byte[] clr) => clr,
            deserialize: (SqliteValue sqlite) => sqlite.AsBlob
        );
        RegisterType<List<byte>>(
            sqliteType: SqliteType.Blob,
            serialize: (List<byte> clr) => clr.ToArray(),
            deserialize: (SqliteValue sqlite) => sqlite.AsBlob.ToList()
        );
        RegisterType<Enum>(
            sqliteType: SqliteType.Integer,
            serialize: (Enum clr) => Convert.ToInt64(clr),
            deserialize: (SqliteValue sqlite) => sqlite.AsInteger
        );
        RegisterType<StringBuilder>(
            sqliteType: SqliteType.Text,
            serialize: (StringBuilder clr) => clr.ToString(),
            deserialize: (SqliteValue sqlite) => sqlite.AsText
        );
        RegisterType<Guid>(
            sqliteType: SqliteType.Text,
            serialize: (Guid clr) => clr.ToString(),
            deserialize: (SqliteValue sqlite) => sqlite.AsText
        );
    }
    private static string GetTypeSql(SqliteType sqliteType) => sqliteType switch {
        SqliteType.Integer => "integer",
        SqliteType.Float => "float",
        SqliteType.Text => "text",
        SqliteType.Blob => "blob",
        SqliteType.Null => "null",
        _ => throw new NotImplementedException()
    };
}

public readonly struct TypeSerializer(Type clrType, SqliteType sqliteType, Func<object, SqliteValue> serialize, Func<SqliteValue, object?> deserialize) {
    public Type ClrType { get; } = clrType;
    public SqliteType SqliteType { get; } = sqliteType;
    public Func<object, SqliteValue> Serialize { get; } = serialize;
    public Func<SqliteValue, object?> Deserialize { get; } = deserialize;
}