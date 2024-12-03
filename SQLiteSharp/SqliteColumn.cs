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

        Collation = Orm.GetCollation(member);

        Check = Orm.GetCheck(member);

        IsPrimaryKey = Orm.IsPrimaryKey(member) || Connection.Orm.IsImplicitPrimaryKey(member);

        IsAutoIncrement = Orm.IsAutoIncrement(member) || (IsPrimaryKey && Connection.Orm.IsImplicitAutoIncrementedPrimaryKey(member));

        Indexes = Orm.GetIndexes(member).ToArray();
        if (Indexes.Length == 0 && !IsPrimaryKey && Connection.Orm.IsImplicitIndex(member)) {
            Indexes = [new IndexedAttribute()];
        }

        IsNotNull = IsPrimaryKey || Orm.IsNotNullConstrained(member);
    }

    public void SetValue(object obj, object? value) {
        switch (ClrMember) {
            case PropertyInfo propertyInfo:
                propertyInfo.SetValue(obj, value);
                break;
            case FieldInfo fieldInfo:
                fieldInfo.SetValue(obj, value);
                break;
            default:
                throw new InvalidProgramException();
        }
    }
    public void SetSqliteValue(object obj, SqliteValue sqliteValue) {
        TypeSerializer typeSerializer = Connection.Orm.GetTypeSerializer(ClrType);
        object? value = typeSerializer.Deserialize(sqliteValue, ClrType);
        SetValue(obj, value);
    }
    public object? GetValue(object obj) {
        return ClrMember switch {
            PropertyInfo propertyInfo => propertyInfo.GetValue(obj),
            FieldInfo fieldInfo => fieldInfo.GetValue(obj),
            _ => throw new InvalidProgramException(),
        };
    }

    private static Type GetMemberType(MemberInfo memberInfo) {
        return memberInfo switch {
            PropertyInfo propertyInfo => propertyInfo.PropertyType,
            FieldInfo fieldInfo => fieldInfo.FieldType,
            _ => throw new InvalidProgramException($"{nameof(SqliteColumn)} only supports properties and fields."),
        };
    }
}