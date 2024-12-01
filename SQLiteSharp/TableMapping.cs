using System.Reflection;

namespace SQLiteSharp;

public class TableMapping {
    public Type MappedType { get; }
    public string TableName { get; }
    public bool WithoutRowId { get; }
    public Column[] Columns { get; }
    public Column? PrimaryKey { get; }
    public CreateFlags CreateFlags { get; }

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
    }

    public string GetByPrimaryKeySql {
        get {
            if (PrimaryKey is null) {
                throw new InvalidOperationException("Table mapping has no primary key");
            }
            return $"select * from {Quote(TableName)} where {Quote(PrimaryKey.Name)} = ?";
        }
    }

    public bool HasAutoIncrementedPrimaryKey => _autoIncrementedPrimaryKey is not null;
    public void SetAutoIncrementedPrimaryKey(object obj, long id) {
        _autoIncrementedPrimaryKey?.SetValue(obj, Convert.ChangeType(id, _autoIncrementedPrimaryKey.Type));
    }

    public Column? FindColumnByMemberName(string memberName) {
        return Columns.FirstOrDefault(column => column.MemberInfo.Name == memberName);
    }
    public Column? FindColumnByColumnName(string columnName) {
        return Columns.FirstOrDefault(column => column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
    }

    public class Column {
        public string Name { get; }
        public MemberInfo MemberInfo { get; }
        public Type Type { get; }
        public string Collation { get; }
        public bool AutoIncrement { get; }
        public bool AutoGuid { get; }
        public bool PrimaryKey { get; }
        public bool NotNull { get; }
        public int? MaxStringLength { get; }
        public bool StoreAsText { get; }
        public IEnumerable<IndexedAttribute> Indices { get; }

        public Column(MemberInfo member, CreateFlags createFlags = CreateFlags.None) {
            MemberInfo = member;
            Type memberType = GetMemberType(member);

            Name = member.GetCustomAttribute<ColumnAttribute>()?.Name ?? member.Name;

            // If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
            Type = Nullable.GetUnderlyingType(memberType) ?? memberType;
            Collation = ObjectMapper.GetCollation(member);

            PrimaryKey = ObjectMapper.IsPrimaryKey(member)
                || (createFlags.HasFlag(CreateFlags.ImplicitPrimaryKey) && string.Equals(member.Name, ObjectMapper.ImplicitPrimaryKeyName, StringComparison.OrdinalIgnoreCase));

            bool isAutoIncrement = ObjectMapper.IsAutoIncrement(member) || (PrimaryKey && createFlags.HasFlag(CreateFlags.AutoIncrementPrimaryKey));
            AutoGuid = isAutoIncrement && Type == typeof(Guid);
            AutoIncrement = isAutoIncrement && !AutoGuid;

            Indices = ObjectMapper.GetIndices(member);
            if (!Indices.Any() && !PrimaryKey && createFlags.HasFlag(CreateFlags.ImplicitIndex) && Name.EndsWith(ObjectMapper.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)) {
                Indices = [new IndexedAttribute()];
            }
            NotNull = PrimaryKey || ObjectMapper.IsMarkedNotNull(member);
            MaxStringLength = ObjectMapper.MaxStringLength(member);

            StoreAsText = memberType.GetCustomAttribute<StoreByNameAttribute>() is not null;
        }

        public void SetValue(object obj, object? value) {
            switch (MemberInfo) {
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
        public object? GetValue(object obj) {
            return MemberInfo switch {
                PropertyInfo propertyInfo => propertyInfo.GetValue(obj),
                FieldInfo fieldInfo => fieldInfo.GetValue(obj),
                _ => throw new InvalidProgramException(),
            };
        }
        private static Type GetMemberType(MemberInfo memberInfo) {
            return memberInfo switch {
                PropertyInfo propertyInfo => propertyInfo.PropertyType,
                FieldInfo fieldInfo => fieldInfo.FieldType,
                _ => throw new InvalidProgramException($"{nameof(TableMapping)} only supports properties and fields."),
            };
        }
    }
}