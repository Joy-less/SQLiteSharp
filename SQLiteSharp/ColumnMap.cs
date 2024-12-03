using System.Reflection;

namespace SQLiteSharp;

public class ColumnMap {
    public Orm Orm { get; }
    public string Name { get; }
    public MemberInfo ClrMember { get; }
    public Type ClrType { get; }
    public string? Collation { get; }
    public string? Check { get; }
    public bool AutoIncrement { get; }
    public bool PrimaryKey { get; }
    public bool NotNull { get; }
    public IndexedAttribute[] Indexes { get; }

    public ColumnMap(MemberInfo member, Orm? orm = null) {
        Orm = orm ?? Orm.Default;

        ClrMember = member;
        ClrType = GetMemberType(member);

        Name = member.GetCustomAttribute<ColumnAttribute>()?.Name ?? member.Name;

        Collation = Orm.GetCollation(member);

        Check = Orm.GetCheck(member);

        PrimaryKey = Orm.IsPrimaryKey(member) || Orm.IsImplicitPrimaryKey(member);

        AutoIncrement = Orm.IsAutoIncrement(member) || (PrimaryKey && Orm.IsImplicitAutoIncrementedPrimaryKey(member));

        Indexes = Orm.GetIndexes(member).ToArray();
        if (Indexes.Length == 0 && !PrimaryKey && Orm.IsImplicitIndex(member)) {
            Indexes = [new IndexedAttribute()];
        }

        NotNull = PrimaryKey || Orm.IsNotNullConstrained(member);
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
        TypeSerializer typeSerializer = Orm.GetTypeSerializer(ClrType);
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
            _ => throw new InvalidProgramException($"{nameof(ColumnMap)} only supports properties and fields."),
        };
    }
}