using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Reflection;
using System.Text.Encodings.Web;

namespace SQLiteSharp;

/// <summary>
/// An Object-Relational Mapper used to map CLR members to SQLite columns.
/// </summary>
public class Orm {
    /// <summary>
    /// The types recognised by the ORM.
    /// </summary>
    /// <remarks>
    /// The types should be non-nullable, since type serializers are not used for <see langword="null"/>.
    /// </remarks>
    public ConcurrentDictionary<Type, TypeSerializer> TypeSerializers { get; } = [];
    /// <summary>
    /// A predicate deciding whether the member should be made the primary key even if it lacks a <see cref="PrimaryKeyAttribute"/>.<br/>
    /// By default, returns <see langword="true"/> if the member's name is "Id".
    /// </summary>
    /// <remarks>
    /// This predicate is ignored if the member has a <see cref="PrimaryKeyAttribute"/>.
    /// </remarks>
    public Func<MemberInfo, bool> IsImplicitPrimaryKey { get; set; } = Member => Member.Name is "Id";
    /// <summary>
    /// A predicate deciding whether an index should be made for the member even if it lacks a <see cref="IndexAttribute"/>.<br/>
    /// By default, returns <see langword="true"/> if the member's name ends with "Id".
    /// </summary>
    /// <remarks>
    /// This predicate is ignored if the member has an <see cref="IndexAttribute"/>.
    /// </remarks>
    public Func<MemberInfo, bool> IsImplicitIndex { get; set; } = Member => Member.Name.EndsWith("Id");
    /// <summary>
    /// A predicate deciding whether the member should be auto-incremented even if it lacks a <see cref="AutoIncrementAttribute"/>.<br/>
    /// By default, always returns <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// This predicate is ignored if the member has an <see cref="AutoIncrementAttribute"/>.
    /// </remarks>
    public Func<MemberInfo, bool> IsImplicitAutoIncremented { get; set; } = Member => false;

    /// <summary>
    /// A global instance of <see cref="Orm"/> used as the default for <see cref="SqliteConnectionOptions.Orm"/>.
    /// </summary>
    public static Orm Default { get; } = new();

    /// <summary>
    /// Constructs a new <see cref="Orm"/> with the default type serializers.
    /// </summary>
    public Orm() {
        AddDefaultTypeSerializers();
    }
    /// <summary>
    /// Creates a type serializer for the given type.
    /// </summary>
    public void RegisterType(Type type, SqliteType sqliteType, Func<object, SqliteValue> serialize, Func<SqliteValue, Type, object?> deserialize) {
        TypeSerializers[type] = new TypeSerializer(type, sqliteType, serialize, deserialize);
    }
    /// <inheritdoc cref="RegisterType(Type, SqliteType, Func{object, SqliteValue}, Func{SqliteValue, Type, object?})"/>
    public void RegisterType<T>(SqliteType sqliteType, Func<T, SqliteValue> serialize, Func<SqliteValue, Type, T?> deserialize) {
        RegisterType(typeof(T), sqliteType, (object clr) => serialize((T)clr), (SqliteValue sqlite, Type clrType) => deserialize(sqlite, clrType));
    }
    /// <summary>
    /// Gets a type serializer for the given (non-nullable) type.<br/>
    /// If not found for the exact type, the type's interfaces are searched.<br/>
    /// If not found for an interface, the type's base types are searched.
    /// </summary>
    public TypeSerializer GetTypeSerializer(Type type) {
        // Get non-nullable type (int? to int)
        type = Nullable.GetUnderlyingType(type) ?? type;

        // Try get serializer for exact type
        if (TypeSerializers.TryGetValue(type, out TypeSerializer typeSerializer)) {
            return typeSerializer;
        }

        // Try get fallback serializer for interface
        foreach (Type fallbackInterface in type.GetInterfaces()) {
            if (TypeSerializers.TryGetValue(fallbackInterface, out typeSerializer)) {
                return typeSerializer;
            }
        }

        // Try get fallback serializer for base type
        Type? fallbackType = type.BaseType;
        while (fallbackType is not null) {
            if (TypeSerializers.TryGetValue(fallbackType, out typeSerializer)) {
                return typeSerializer;
            }
            fallbackType = fallbackType.BaseType;
        }

        // Serializer not found
        throw new InvalidOperationException($"No {nameof(TypeSerializer)} found for '{type}'");
    }
    /// <summary>
    /// Gets a type serializer for the given object's type and serializes the object as a <see cref="SqliteValue"/>.
    /// </summary>
    public SqliteValue Serialize(object? clr) {
        if (clr is null) {
            return SqliteValue.Null;
        }
        return GetTypeSerializer(clr.GetType()).Serialize(clr);
    }
    /// <summary>
    /// Gets a type serializer for the given type and deserializes the object from a <see cref="SqliteValue"/>.
    /// </summary>
    public object? Deserialize(SqliteValue sqlite, Type clrType) {
        if (sqlite.IsNull) {
            return null;
        }
        return GetTypeSerializer(clrType).Deserialize(sqlite, clrType);
    }
    /// <summary>
    /// Gets a SQL declaration string for the column (e.g. <c>name text unique not null</c>).
    /// </summary>
    public string GetSqlDeclaration(SqliteColumn column) {
        TypeSerializer typeSerializer = GetTypeSerializer(column.ClrType);

        string declaration = $"{column.Name.SqlQuote()} {typeSerializer.SqliteType.ToString().SqlQuote()}";

        if (column.IsPrimaryKey) {
            declaration += " primary key";
        }
        if (column.IsAutoIncremented) {
            declaration += " autoincrement";
        }
        if (column.IsNotNull) {
            declaration += " not null";
        }
        if (column.IsUnique) {
            declaration += " unique";
        }
        if (column.Collation is not null) {
            declaration += $" collate {column.Collation.SqlQuote()}";
        }
        if (column.Check is not null) {
            declaration += $" check {column.Check.SqlQuote("'")}";
        }
        if (column.ForeignKey is not null) {
            declaration += $" references {column.ForeignKey.ForeignTable.SqlQuote()}({column.ForeignKey.ForeignColumn.SqlQuote()})"
                + $" on delete {column.ForeignKey.OnDelete.ToEnumString()}"
                + $" on update {column.ForeignKey.OnUpdate.ToEnumString()}";
        }

        return declaration;
    }

    private void AddDefaultTypeSerializers() {
        JsonSerializerOptions jsonOptions = new() {
            NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals | JsonNumberHandling.AllowReadingFromString,
            AllowTrailingCommas = true,
            IncludeFields = true,
            NewLine = "\n",
            ReadCommentHandling = JsonCommentHandling.Skip,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };
        RegisterType<object>(
            sqliteType: SqliteType.Text,
            serialize: (object clr) => JsonSerializer.Serialize(clr, jsonOptions),
            deserialize: (SqliteValue sqlite, Type clrType) => JsonSerializer.Deserialize(sqlite.CastText, clrType, jsonOptions)!
        );

        RegisterType<SqliteValue>(
            SqliteType.Any,
            serialize: (SqliteValue clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite
        );
        RegisterType<bool>(
            SqliteType.Integer,
            serialize: (bool clr) => clr ? 1 : 0,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.CastInteger != 0
        );
        RegisterType<string>(
            SqliteType.Text,
            serialize: (string clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.CastText
        );
        RegisterType<byte>(
            SqliteType.Integer,
            serialize: (byte clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (byte)sqlite.CastInteger
        );
        RegisterType<sbyte>(
            SqliteType.Integer,
            serialize: (sbyte clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (sbyte)sqlite.CastInteger
        );
        RegisterType<short>(
            SqliteType.Integer,
            serialize: (short clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (short)sqlite.CastInteger
        );
        RegisterType<ushort>(
            SqliteType.Integer,
            serialize: (ushort clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (ushort)sqlite.CastInteger
        );
        RegisterType<int>(
            SqliteType.Integer,
            serialize: (int clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (int)sqlite.CastInteger
        );
        RegisterType<uint>(
            SqliteType.Integer,
            serialize: (uint clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (uint)sqlite.CastInteger
        );
        RegisterType<long>(
            SqliteType.Integer,
            serialize: (long clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.CastInteger
        );
        RegisterType<ulong>(
            SqliteType.Integer,
            serialize: (ulong clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (ulong)sqlite.CastInteger
        );
        RegisterType<char>(
            SqliteType.Integer,
            serialize: (char clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (char)sqlite.CastInteger
        );
        RegisterType<float>(
            SqliteType.Float,
            serialize: (float clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (float)sqlite.CastFloat
        );
        RegisterType<double>(
            SqliteType.Float,
            serialize: (double clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.CastFloat
        );
#if NET5_0_OR_GREATER
        RegisterType<Half>(
            SqliteType.Float,
            serialize: (Half clr) => (double)clr,
            deserialize: (SqliteValue sqlite, Type clrType) => (Half)sqlite.CastFloat
        );
#endif
        RegisterType<TimeSpan>(
            SqliteType.Integer,
            serialize: (TimeSpan clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite, Type clrType) => new TimeSpan(sqlite.CastInteger)
        );
        RegisterType<DateTime>(
            SqliteType.Integer,
            serialize: (DateTime clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite, Type clrType) => new DateTime(sqlite.CastInteger)
        );
#if NET6_0_OR_GREATER
        RegisterType<DateOnly>(
            SqliteType.Integer,
            serialize: (DateOnly clr) => clr.ToDateTime(TimeOnly.MinValue).Ticks,
            deserialize: (SqliteValue sqlite, Type clrType) => DateOnly.FromDateTime(new DateTime(sqlite.CastInteger))
        );
        RegisterType<TimeOnly>(
            SqliteType.Integer,
            serialize: (TimeOnly clr) => clr.Ticks,
            deserialize: (SqliteValue sqlite, Type clrType) => new TimeOnly(sqlite.CastInteger)
        );
#endif
        RegisterType<Uri>(
            SqliteType.Text,
            serialize: (Uri clr) => clr.AbsoluteUri,
            deserialize: (SqliteValue sqlite, Type clrType) => new Uri(sqlite.CastText)
        );
        RegisterType<byte[]>(
            SqliteType.Blob,
            serialize: (byte[] clr) => clr,
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.CastBlob
        );
        RegisterType<IEnumerable<byte>>(
            SqliteType.Blob,
            serialize: (IEnumerable<byte> clr) => clr.ToArray(),
            deserialize: (SqliteValue sqlite, Type clrType) => sqlite.CastBlob
        );
        RegisterType<Enum>(
            SqliteType.Integer,
            serialize: (Enum clr) => Convert.ToInt64(clr),
            deserialize: (SqliteValue sqlite, Type clrType) => (Enum)Enum.ToObject(clrType, sqlite.CastInteger)
        );
        RegisterType<StringBuilder>(
            SqliteType.Text,
            serialize: (StringBuilder clr) => clr.ToString(),
            deserialize: (SqliteValue sqlite, Type clrType) => new StringBuilder(sqlite.CastText)
        );
        RegisterType<Guid>(
            SqliteType.Text,
            serialize: (Guid clr) => clr.ToString(),
            deserialize: (SqliteValue sqlite, Type clrType) => Guid.Parse(sqlite.CastText)
        );
    }
}

/// <summary>
/// Contains functions to convert between <see cref="object"/> and <see cref="SqliteValue"/> for a specific type.
/// </summary>
public readonly struct TypeSerializer(Type clrType, SqliteType sqliteType, Func<object, SqliteValue> serialize, Func<SqliteValue, Type, object?> deserialize) {
    /// <summary>
    /// The CLR (.NET) type used in the program.
    /// </summary>
    public Type ClrType { get; } = clrType;
    /// <summary>
    /// The SQLite type used in the database.
    /// </summary>
    public SqliteType SqliteType { get; } = sqliteType;
    /// <summary>
    /// Serializes the object from <see cref="ClrType"/> to <see cref="SqliteType"/>.
    /// </summary>
    public Func<object, SqliteValue> Serialize { get; } = serialize;
    /// <summary>
    /// Serializes the object from <see cref="SqliteType"/> to the desired type (which should be compatible with <see cref="ClrType"/>).
    /// </summary>
    public Func<SqliteValue, Type, object?> Deserialize { get; } = deserialize;
}