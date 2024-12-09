using System.Reflection;

namespace SQLiteSharp;

public class SqliteColumn {
    public SqliteConnection Connection { get; }
    public string Name { get; }
    public MemberInfo ClrMember { get; }
    public Type ClrType { get; }
    public string? Collation { get; }
    public string? Check { get; }
    public bool IsAutoIncremented { get; }
    public bool IsPrimaryKey { get; }
    public bool IsNotNull { get; }
    public bool IsUnique { get; }
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

        IsAutoIncremented = member.GetCustomAttribute<AutoIncrementAttribute>() is not null
            || Connection.Orm.IsImplicitAutoIncremented(member);

        Indexes = [.. member.GetCustomAttributes<IndexAttribute>()];
        if (Indexes.Length == 0 && Connection.Orm.IsImplicitIndex(member)) {
            Indexes = [new IndexAttribute()];
        }

        IsUnique = member.GetCustomAttribute<UniqueAttribute>() is not null;

        IsNotNull = member.GetCustomAttribute<NotNullAttribute>() is not null
            || IsPrimaryKey;
    }

    public void SetValue(object row, object? value) {
        ClrMember.SetValue(row, value);
    }
    public void SetSqliteValue(object row, SqliteValue sqliteValue) {
        SetValue(row, Connection.Orm.Deserialize(ClrType, sqliteValue));
    }
    public object? GetValue(object row) {
        return ClrMember.GetValue(row);
    }
    public SqliteValue GetSqliteValue(object row) {
        return Connection.Orm.Serialize(GetValue(row));
    }

    private static Type GetMemberType(MemberInfo memberInfo) {
        return memberInfo switch {
            PropertyInfo propertyInfo => propertyInfo.PropertyType,
            FieldInfo fieldInfo => fieldInfo.FieldType,
            _ => throw new InvalidProgramException("Member must be a property or a field."),
        };
    }
}