using System.Collections;
using System.Linq.Expressions;
using System.Text;

namespace SQLiteSharp;

public class SqlBuilder2<T>(SqliteTable<T> table) where T : notnull, new() {
    public SqliteTable<T> Table { get; } = table;
    public Dictionary<string, object?> Parameters { get; } = [];

    private readonly List<string> SelectList = [];
    private readonly List<string> OrderByList = [];
    private readonly List<string> GroupByList = [];
    private readonly List<string> WhereList = [];
    private readonly Dictionary<string, string> UpdateList = [];
    private readonly Dictionary<string, string> InsertList = [];
    private bool DeleteFlag = false;
    private long LimitCount = -1;
    private long OffsetCount = -1;

    private int CurrentParameterIndex;

    public SqlBuilder2<T> Select() {
        SelectList.Add($"{Table.Name.SqlQuote()}.*");
        return this;
    }
    public SqlBuilder2<T> Select(string columnName) {
        SelectList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    public SqlBuilder2<T> Select(string columnName, SelectType selectType) {
        SelectList.Add($"{selectType}({Table.Name.SqlQuote()}.{columnName.SqlQuote()})");
        return this;
    }
    public SqlBuilder2<T> Select(SelectType selectType) {
        SelectList.Add($"{selectType}(*)");
        return this;
    }
    public SqlBuilder2<T> OrderBy(string columnName) {
        OrderByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    public SqlBuilder2<T> OrderByDescending(string columnName) {
        OrderByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} desc");
        return this;
    }
    public SqlBuilder2<T> GroupBy(string columnName) {
        GroupByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    public SqlBuilder2<T> Where(string columnName, string @operator, object? value) {
        WhereList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} {@operator} {AddParameter(value)}");
        return this;
    }
    public SqlBuilder2<T> WhereNull(string columnName) {
        WhereList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} is null");
        return this;
    }
    public SqlBuilder2<T> WhereNotNull(string columnName) {
        WhereList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} is not null");
        return this;
    }
    public SqlBuilder2<T> WhereIn(string columnName, IEnumerable values, bool negate = false) {
        IEnumerable<string> parameterNames = values.Cast<object?>().Select(AddParameter);
        WhereList.Add($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} in ({string.Join(",", parameterNames)})");
        return this;
    }
    public SqlBuilder2<T> WhereNotIn(string columnName, IEnumerable values) {
        WhereIn(columnName, values, negate: true);
        return this;
    }
    public SqlBuilder2<T> Take(long count) {
        LimitCount = count;
        return this;
    }
    public SqlBuilder2<T> Skip(long count) {
        OffsetCount = count;
        return this;
    }
    public SqlBuilder2<T> Update(string columnName, string updateExpression) {
        UpdateList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", updateExpression);
        return this;
    }
    public SqlBuilder2<T> Insert(string columnName, object? value) {
        InsertList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", AddParameter(value));
        return this;
    }
    /*public void Insert(IDictionary<string, object?> columnValues) {
        foreach (KeyValuePair<string, object?> columnValue in columnValues) {
            Insert(columnValue.Key, columnValue.Value);
        }
    }*/
    public SqlBuilder2<T> Delete() {
        DeleteFlag = true;
        return this;
    }

    public string Generate() {
        StringBuilder builder = new();

        if (SelectList.Count > 0) {
            builder.AppendLine($"select {string.Join(",", SelectList)} from {Table.Name.SqlQuote()}");
            if (OrderByList.Count > 0) {
                builder.AppendLine($"order by {string.Join(",", OrderByList)}");
            }
            if (GroupByList.Count > 0) {
                builder.AppendLine($"group by {string.Join(",", GroupByList)}");
            }
            if (WhereList.Count > 0) {
                builder.AppendLine($"where {string.Join(",", WhereList)}");
            }
            if (LimitCount >= 0) {
                builder.AppendLine($"limit {LimitCount}");
            }
            if (OffsetCount >= 0) {
                builder.AppendLine($"offset {OffsetCount}");
            }
            builder.AppendLine(";");
        }
        if (UpdateList.Count > 0) {
            List<KeyValuePair<string, string>> updateList = [.. UpdateList];
            builder.AppendLine($"update {Table.Name.SqlQuote()}");
            builder.AppendLine($"set {string.Join(",", updateList.Select(update => $"{update.Key} = {update.Value}"))}");
            if (WhereList.Count > 0) {
                builder.AppendLine($"where {string.Join(",", WhereList)}");
            }
            if (LimitCount >= 0) {
                builder.AppendLine($"limit {LimitCount}");
            }
            if (OffsetCount >= 0) {
                builder.AppendLine($"offset {OffsetCount}");
            }
            builder.AppendLine(";");
        }
        if (InsertList.Count > 0) {
            List<KeyValuePair<string, string>> insertList = [.. InsertList];
            builder.AppendLine($"insert into {Table.Name.SqlQuote()}");
            builder.AppendLine($"({insertList.Select(insert => insert.Key)})");
            builder.AppendLine($"values ({insertList.Select(insert => insert.Value)})");
            if (WhereList.Count > 0) {
                builder.AppendLine($"where {string.Join(",", WhereList)}");
            }
            if (LimitCount >= 0) {
                builder.AppendLine($"limit {LimitCount}");
            }
            if (OffsetCount >= 0) {
                builder.AppendLine($"offset {OffsetCount}");
            }
            builder.AppendLine(";");
        }
        if (DeleteFlag) {
            builder.AppendLine($"delete from {Table.Name.SqlQuote()}");
            if (WhereList.Count > 0) {
                builder.AppendLine($"where {string.Join(",", WhereList)}");
            }
            if (LimitCount >= 0) {
                builder.AppendLine($"limit {LimitCount}");
            }
            if (OffsetCount >= 0) {
                builder.AppendLine($"offset {OffsetCount}");
            }
        }

        return builder.ToString();
    }

    public SqlBuilder2<T> Select(MemberExpression member) {
        string columnName = Table.MemberNameToColumnName(member.Member.Name);
        Select(columnName);
        return this;
    }
    public SqlBuilder2<T> Select(Expression<Func<T, object?>> member) {
        if (member.Body is not MemberExpression memberExpression) {
            throw new ArgumentException(null, nameof(member));
        }
        Select(memberExpression);
        return this;
    }
    public SqlBuilder2<T> Select(MemberExpression member, SelectType selectType) {
        string columnName = Table.MemberNameToColumnName(member.Member.Name);
        Select(columnName, selectType);
        return this;
    }
    public SqlBuilder2<T> Select(Expression<Func<T, object?>> member, SelectType selectType) {
        if (member.Body is not MemberExpression memberExpression) {
            throw new ArgumentException(null, nameof(member));
        }
        Select(memberExpression.Member.Name, selectType);
        return this;
    }
    public SqlBuilder2<T> OrderBy(MemberExpression member) {
        string columnName = Table.MemberNameToColumnName(member.Member.Name);
        OrderBy(columnName);
        return this;
    }
    public SqlBuilder2<T> OrderBy(Expression<Func<T, object?>> member) {
        if (member.Body is not MemberExpression memberExpression) {
            throw new ArgumentException(null, nameof(member));
        }
        OrderBy(memberExpression);
        return this;
    }
    public SqlBuilder2<T> OrderByDescending(MemberExpression member) {
        string columnName = Table.MemberNameToColumnName(member.Member.Name);
        OrderByDescending(columnName);
        return this;
    }
    public SqlBuilder2<T> OrderByDescending(Expression<Func<T, object?>> member) {
        if (member.Body is not MemberExpression memberExpression) {
            throw new ArgumentException(null, nameof(member));
        }
        OrderByDescending(memberExpression);
        return this;
    }
    public SqlBuilder2<T> GroupBy(MemberExpression member) {
        string columnName = Table.MemberNameToColumnName(member.Member.Name);
        GroupBy(columnName);
        return this;
    }
    public SqlBuilder2<T> GroupBy(Expression<Func<T, object?>> member) {
        if (member.Body is not MemberExpression memberExpression) {
            throw new ArgumentException(null, nameof(member));
        }
        GroupBy(memberExpression);
        return this;
    }

    public static string SqlOperator(ExpressionType operatorType) => operatorType switch {
        ExpressionType.GreaterThan => ">",
        ExpressionType.GreaterThanOrEqual => ">=",
        ExpressionType.LessThan => "<",
        ExpressionType.LessThanOrEqual => "<=",
        ExpressionType.And => "&",
        ExpressionType.AndAlso => "and",
        ExpressionType.Or => "|",
        ExpressionType.OrElse => "or",
        ExpressionType.Equal => "=",
        ExpressionType.NotEqual => "!=",
        ExpressionType.Add => "+",
        ExpressionType.Subtract => "-",
        ExpressionType.Multiply => "*",
        ExpressionType.Divide => "/",
        ExpressionType.Modulo => "%",
        ExpressionType.OnesComplement => "~",
        ExpressionType.LeftShift => "<<",
        ExpressionType.RightShift => ">>",
        _ => throw new NotSupportedException($"Cannot get SQL operator for {operatorType}")
    };

    private string GenerateParameterName() {
        CurrentParameterIndex++;
        return $"@p{CurrentParameterIndex}";
    }
    private string AddParameter(object? value) {
        string name = GenerateParameterName();
        Parameters.Add(name, value);
        return name;
    }
}

/// <summary>
/// SQL aggregate functions (e.g. <c>SELECT COUNT(*)</c>)<br/>
/// See <see href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</see>.
/// </summary>
public enum SelectType {
    AVG,
    COUNT,
    MIN,
    MAX,
    SUM,
    TOTAL,
}