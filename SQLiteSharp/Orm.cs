using System.Collections.Concurrent;
using System.Text;
using System.Reflection;

namespace SQLiteSharp;

public class Orm {
    public ConcurrentDictionary<Type, TypeSerializer> TypeSerializers { get; } = [];
    public Func<MemberInfo, bool> IsImplicitPrimaryKey { get; set; } = Member => Member.Name == "Id";
    public Func<MemberInfo, bool> IsImplicitIndex { get; set; } = Member => Member.Name.EndsWith("Id");
    public Func<MemberInfo, bool> IsImplicitAutoIncrementedPrimaryKey { get; set; } = Member => false;

    public static Orm Default { get; } = new();

    public Orm() {
        AddDefaultTypeSerializers();
    }
    public void RegisterType(Type type, SqliteType sqliteType, Func<object, SqliteValue> serialize, Func<SqliteValue, Type, object?> deserialize) {
        TypeSerializers[type] = new TypeSerializer(type, sqliteType, serialize, deserialize);
    }
    public void RegisterType<T>(SqliteType sqliteType, Func<T, SqliteValue> serialize, Func<SqliteValue, Type, object?> deserialize) {
        RegisterType(typeof(T), sqliteType, (object clr) => serialize((T)clr), (SqliteValue sqlite, Type clrType) => deserialize(sqlite, clrType));
    }
    public bool UnregisterType(Type type) {
        return TypeSerializers.TryRemove(type, out _);
    }
    public bool UnregisterType<T>() {
        return TypeSerializers.TryRemove(typeof(T), out _);
    }
    public TypeSerializer GetTypeSerializer(Type type) {
        // Get non-nullable type (int? to int)
        type = type.AsNotNullable();

        // Try get serializer for exact type
        if (TypeSerializers.TryGetValue(type, out TypeSerializer typeSerializer)) {
            return typeSerializer;
        }

        // Try get fallback serializer for base type
        Type? fallbackType = type.BaseType;
        while (fallbackType is not null) {
            if (TypeSerializers.TryGetValue(fallbackType, out typeSerializer)) {
                return typeSerializer;
            }
            fallbackType = type.BaseType;
        }

        // Serializer not found
        throw new InvalidOperationException($"No {nameof(TypeSerializer)} found for '{type}'");
    }
    public string GetSqlDeclaration(SqliteColumn column) {
        TypeSerializer typeSerializer = GetTypeSerializer(column.ClrType);

        string declaration = $"{column.Name.SqlQuote()} {GetTypeSql(typeSerializer.SqliteType).SqlQuote()} ";

        if (column.IsPrimaryKey) {
            declaration += "primary key ";
        }
        if (column.IsAutoIncrement) {
            declaration += "autoincrement ";
        }
        if (column.IsNotNull) {
            declaration += "not null ";
        }
        if (column.Collation is not null) {
            declaration += $"collate {column.Collation.SqlQuote()} ";
        }
        if (column.Check is not null) {
            declaration += $"check ({column.Check.SqlQuote()}) ";
        }

        return declaration;
    }

    private void AddDefaultTypeSerializers() {
        RegisterType<bool>(
            SqliteType.Integer,
            serialize: (bool clr) => clr ? 1 : 0,
            deserialize: (SqliteValue sqlite, Type clrType) => (int)sqlite.AsInteger != 0
        );
        RegisterType<string>(
            SqliteType.Text,
            serialize: (string clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsText
        );
        RegisterType<byte>(
            SqliteType.Integer,
            serialize: (byte clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (byte)sqlite.AsInteger
        );
        RegisterType<sbyte>(
            SqliteType.Integer,
            serialize: (sbyte clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (sbyte)sqlite.AsInteger
        );
        RegisterType<short>(
            SqliteType.Integer,
            serialize: (short clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (short)sqlite.AsInteger
        );
        RegisterType<ushort>(
            SqliteType.Integer,
            serialize: (ushort clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (ushort)sqlite.AsInteger
        );
        RegisterType<int>(
            SqliteType.Integer,
            serialize: (int clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (int)sqlite.AsInteger
        );
        RegisterType<uint>(
            SqliteType.Integer,
            serialize: (uint clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (uint)sqlite.AsInteger
        );
        RegisterType<long>(
            SqliteType.Integer,
            serialize: (long clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsInteger
        );
        RegisterType<ulong>(
            SqliteType.Integer,
            serialize: (ulong clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (ulong)sqlite.AsInteger
        );
        RegisterType<char>(
            SqliteType.Integer,
            serialize: (char clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsInteger
        );
        RegisterType<float>(
            SqliteType.Integer,
            serialize: (float clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (float)sqlite.AsFloat
        );
        RegisterType<double>(
            SqliteType.Integer,
            serialize: (double clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsFloat
        );
        RegisterType<TimeSpan>(
            SqliteType.Integer,
            serialize: (TimeSpan clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite, Type clrType) => new TimeSpan(sqlite.AsInteger)
        );
        RegisterType<DateTime>(
            SqliteType.Integer,
            serialize: (DateTime clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite, Type clrType) => new DateTime(sqlite.AsInteger)
        );
        RegisterType<Uri>(
            SqliteType.Text,
            serialize: (Uri clr) => clr.AbsoluteUri,
            deserialize: (SqliteValue sqlite, Type clrType) => new Uri(sqlite.AsText)
        );
        RegisterType<byte[]>(
            SqliteType.Blob,
            serialize: (byte[] clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsBlob
        );
        RegisterType<IEnumerable<byte>>(
            SqliteType.Blob,
            serialize: (IEnumerable<byte> clr) => clr.ToArray(),
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsBlob.ToList()
        );
        RegisterType<Enum>(
            SqliteType.Integer,
            serialize: (Enum clr) => Convert.ToInt64(clr),
            deserialize: (SqliteValue sqlite, Type clrType) => Enum.ToObject(clrType, sqlite.AsInteger)
        );
        RegisterType<StringBuilder>(
            SqliteType.Text,
            serialize: (StringBuilder clr) => clr.ToString(),
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsText
        );
        RegisterType<Guid>(
            SqliteType.Text,
            serialize: (Guid clr) => clr.ToString(),
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.AsText
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

public readonly struct TypeSerializer(Type clrType, SqliteType sqliteType, Func<object, SqliteValue> serialize, Func<SqliteValue, Type, object?> deserialize) {
    public Type ClrType { get; } = clrType;
    public SqliteType SqliteType { get; } = sqliteType;
    public Func<object, SqliteValue> Serialize { get; } = serialize;
    public Func<SqliteValue, Type, object?> Deserialize { get; } = deserialize;
}