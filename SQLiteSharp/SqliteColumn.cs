using System.Reflection;

namespace SQLiteSharp;

public class SqliteColumn {
    public SqliteConnection Connection { get; }
    public string Name { get; }
    public MemberInfo ClrMember { get; }
    public Type ClrType { get; }
    public string? Collation { get; }
    public string? Check { get; }
    public bool IsAutoIncrement { get; }
    public bool IsPrimaryKey { get; }
    public bool IsNotNull { get; }
    public IndexedAttribute[] Indexes { get; }

    public SqliteColumn(SqliteConnection connection, MemberInfo member) {
        Connection = connection;

        ClrMember = member;
        ClrType = GetMemberType(member);

        Name = member.GetCustomAttribute<ColumnAttribute>()?.Name ?? member.Name;

        Collation = member.GetCustomAttribute<CollationAttribute>()?.Value;

        Check = member.GetCustomAttribute<CheckAttribute>()?.Value;

        IsPrimaryKey = member.GetCustomAttribute<PrimaryKeyAttribute>() is not null
            || Connection.Orm.IsImplicitPrimaryKey(member);

        IsAutoIncrement = member.GetCustomAttribute<AutoIncrementAttribute>() is not null
            || (IsPrimaryKey && Connection.Orm.IsImplicitAutoIncrementedPrimaryKey(member));

        Indexes = [.. member.GetCustomAttributes<IndexedAttribute>()];
        if (Indexes.Length == 0 && !IsPrimaryKey && Connection.Orm.IsImplicitIndex(member)) {
            Indexes = [new IndexedAttribute()];
        }

        IsNotNull = member.GetCustomAttribute<NotNullAttribute>() is not null
            || IsPrimaryKey;
    }

    public void SetValue(object row, object? value) {
        switch (ClrMember) {
            case PropertyInfo propertyInfo:
                propertyInfo.SetValue(row, value);
                break;
            case FieldInfo fieldInfo:
                fieldInfo.SetValue(row, value);
                break;
            default:
                throw new InvalidProgramException();
        }
    }
    public void SetSqliteValue(object row, SqliteValue sqliteValue) {
        TypeSerializer typeSerializer = Connection.Orm.GetTypeSerializer(ClrType);
        object? value = typeSerializer.Deserialize(sqliteValue, ClrType);
        SetValue(row, value);
    }
    public object? GetValue(object row) {
        return ClrMember switch {
            PropertyInfo propertyInfo => propertyInfo.GetValue(row),
            FieldInfo fieldInfo => fieldInfo.GetValue(row),
            _ => throw new InvalidProgramException(),
        };
    }
    public SqliteValue GetSqliteValue(object row) {
        object? value = GetValue(row);
        TypeSerializer typeSerializer = Connection.Orm.GetTypeSerializer(ClrType);
        return typeSerializer.Serialize(row);
    }

    private static Type GetMemberType(MemberInfo memberInfo) {
        return memberInfo switch {
            PropertyInfo propertyInfo => propertyInfo.PropertyType,
            FieldInfo fieldInfo => fieldInfo.FieldType,
            _ => throw new InvalidProgramException("Member must be a property or a field."),
        };
    }
}