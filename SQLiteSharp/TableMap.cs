using System.Reflection;

namespace SQLiteSharp;

public class TableMap {
    public Type Type { get; }
    public Orm Orm { get; }
    public string TableName { get; }
    public bool WithoutRowId { get; }
    public ColumnMap[] Columns { get; }
    public ColumnMap? PrimaryKey { get; }
    public bool HasAutoIncrementedPrimaryKey { get; }

    public TableMap(Type type, Orm? orm = null) {
        Type = type;
        Orm = orm ?? Orm.Default;

        TableAttribute? tableAttribute = type.GetCustomAttribute<TableAttribute>();

        TableName = tableAttribute?.Name ?? Type.Name;
        WithoutRowId = tableAttribute?.WithoutRowId ?? false;

        MemberInfo[] members = [.. type.GetProperties(), .. type.GetFields()];
        List<ColumnMap> columns = new(members.Length);
        foreach (MemberInfo member in members) {
            bool ignore = member.GetCustomAttribute<IgnoreAttribute>() is not null;
            if (!ignore) {
                columns.Add(new ColumnMap(member, orm));
            }
        }
        Columns = [.. columns];
        foreach (ColumnMap column in Columns) {
            if (column.PrimaryKey) {
                PrimaryKey = column;
                if (column.AutoIncrement) {
                    HasAutoIncrementedPrimaryKey = true;
                }
            }
        }
    }

    public string GetByPrimaryKeySql {
        get {
            if (PrimaryKey is null) {
                throw new InvalidOperationException("Table mapping has no primary key");
            }
            return $"select * from {TableName.SqlQuote()} where {PrimaryKey.Name.SqlQuote()} = ?";
        }
    }

    public void SetPrimaryKeyValue(object obj, SqliteValue rawValue) {
        PrimaryKey?.SetSqliteValue(obj, rawValue);
    }

    public ColumnMap? FindColumnByMemberName(string memberName) {
        return Columns.FirstOrDefault(column => column.ClrMember.Name == memberName);
    }
    public ColumnMap? FindColumnByColumnName(string columnName) {
        return Columns.FirstOrDefault(column => column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
    }
}