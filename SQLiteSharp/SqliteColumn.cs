using System.Reflection;

namespace SQLiteSharp;

/// <summary>
/// A single column in a <see cref="SqliteTable{T}"/> mapped to a CLR member.
/// </summary>
public class SqliteColumn {
    /// <summary>
    /// The database connection connected to the column.
    /// </summary>
    public SqliteConnection Connection { get; }
    /// <summary>
    /// The name of the column.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// The mapped CLR member (property/field).
    /// </summary>
    public MemberInfo ClrMember { get; }
    /// <summary>
    /// The CLR type of the mapped CLR member (property/field).
    /// </summary>
    public Type ClrType { get; }
    /// <summary>
    /// The collation name to use with the column.
    /// </summary>
    public string? Collation { get; }
    /// <summary>
    /// The check expression constraint for the column.
    /// </summary>
    public string? Check { get; }
    /// <summary>
    /// Whether the column is the primary key of the table.
    /// </summary>
    public bool IsPrimaryKey { get; }
    /// <summary>
    /// The foreign key reference for the column.
    /// </summary>
    public ForeignKeyAttribute? ForeignKey { get; }
    /// <summary>
    /// Whether the column is automatically incremented on insert.
    /// </summary>
    public bool IsAutoIncremented { get; }
    /// <summary>
    /// Whether the column value is required to be non-null.
    /// </summary>
    public bool IsNotNull { get; }
    /// <summary>
    /// Whether the column is required to have a unique value for each row.
    /// </summary>
    public bool IsUnique { get; }
    /// <summary>
    /// The indexes to create in the table.
    /// </summary>
    public IndexAttribute[] Indexes { get; }

    internal SqliteColumn(SqliteConnection connection, MemberInfo member) {
        Connection = connection;

        ClrMember = member;
        ClrType = GetMemberType(member);

        Name = member.GetCustomAttribute<AliasAttribute>()?.Name ?? member.Name;

        Collation = member.GetCustomAttribute<CollationAttribute>()?.Value;

        Check = member.GetCustomAttribute<CheckAttribute>()?.Value;

        IsPrimaryKey = member.GetCustomAttribute<PrimaryKeyAttribute>() is not null
            || Connection.Orm.IsImplicitPrimaryKey(member);

        ForeignKey = member.GetCustomAttribute<ForeignKeyAttribute>();

        IsAutoIncremented = member.GetCustomAttribute<AutoIncrementAttribute>() is not null
            || Connection.Orm.IsImplicitAutoIncrement(member);

        IsNotNull = member.GetCustomAttribute<NotNullAttribute>() is not null
            || IsPrimaryKey;

        IsUnique = member.GetCustomAttribute<UniqueAttribute>() is not null;

        Indexes = [.. member.GetCustomAttributes<IndexAttribute>()];
        if (Indexes.Length == 0 && Connection.Orm.IsImplicitIndex(member)) {
            Indexes = [new IndexAttribute()];
        }
    }

    /// <summary>
    /// Sets the row's member mapped to this column.
    /// </summary>
    public void SetValue(object row, object? value) {
        ClrMember.SetValue(row, value);
    }
    /// <inheritdoc cref="SetValue(object, object?)"/>
    public void SetSqliteValue(object row, SqliteValue sqliteValue) {
        SetValue(row, Connection.Orm.Deserialize(sqliteValue, ClrType));
    }
    /// <summary>
    /// Gets the row's member mapped to this column.
    /// </summary>
    public object? GetValue(object row) {
        return ClrMember.GetValue(row);
    }
    /// <inheritdoc cref="GetValue(object)"/>
    public SqliteValue GetSqliteValue(object row) {
        return Connection.Orm.Serialize(GetValue(row));
    }

    private static Type GetMemberType(MemberInfo memberInfo) {
        return memberInfo switch {
            PropertyInfo propertyInfo => propertyInfo.PropertyType,
            FieldInfo fieldInfo => fieldInfo.FieldType,
            _ => throw new NotSupportedException("Member must be a property or a field."),
        };
    }
}