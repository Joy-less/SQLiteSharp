using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLiteSharp;

public class SqlBuilder<T> where T : notnull, new() {
    public SqliteTable<T> Table { get; }
    public Dictionary<string, object?> Parameters { get; } = [];
    public Dictionary<MethodInfo, MethodToSqlConverter<T>> MethodToSqlConverters { get; } = [];

    private readonly List<string> SelectList = [];
    private readonly List<string> OrderByList = [];
    private readonly List<string> GroupByList = [];
    private readonly List<string> WhereList = [];
    private readonly List<string> HavingList = [];
    private readonly Dictionary<string, string> UpdateList = [];
    private readonly Dictionary<string, string> InsertList = [];
    private bool DeleteFlag = false;
    private long LimitCount = -1;
    private long OffsetCount = -1;

    private int CurrentParameterIndex;

    public SqlBuilder(SqliteTable<T> table) {
        Table = table;

        AddDefaultMethodToSqlConverters();
    }
    public SqlBuilder<T> Select() {
        SelectList.Add($"{Table.Name.SqlQuote()}.*");
        return this;
    }
    public SqlBuilder<T> Select(string columnName) {
        SelectList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    public SqlBuilder<T> Select(string columnName, SelectType selectType) {
        SelectList.Add($"{selectType}({Table.Name.SqlQuote()}.{columnName.SqlQuote()})");
        return this;
    }
    public SqlBuilder<T> Select(SelectType selectType) {
        SelectList.Add($"{selectType}(*)");
        return this;
    }
    public SqlBuilder<T> OrderBy(string columnName) {
        OrderByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    public SqlBuilder<T> OrderByDescending(string columnName) {
        OrderByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} desc");
        return this;
    }
    public SqlBuilder<T> GroupBy(string columnName) {
        GroupByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    public SqlBuilder<T> Where(string condition) {
        WhereList.Add(condition);
        return this;
    }
    public SqlBuilder<T> Having(string condition) {
        HavingList.Add(condition);
        return this;
    }
    public SqlBuilder<T> Take(long count) {
        LimitCount = count;
        return this;
    }
    public SqlBuilder<T> Skip(long count) {
        OffsetCount = count;
        return this;
    }
    public SqlBuilder<T> Update(string columnName, string newValueExpression) {
        UpdateList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", newValueExpression);
        return this;
    }
    public SqlBuilder<T> Insert(string columnName, object? value) {
        InsertList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", AddParameter(value));
        return this;
    }
    public SqlBuilder<T> Delete() {
        DeleteFlag = true;
        return this;
    }

    public string GetCommand() {
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
            if (HavingList.Count > 0) {
                builder.AppendLine($"having {string.Join(",", HavingList)}");
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

    /// <inheritdoc cref="SqliteConnection.Execute(string, IEnumerable{object?})"/>
    public int Execute() {
        return Table.Connection.Execute(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteAsync(string, IDictionary{string, object?})"/>
    public Task<int> ExecuteAsync() {
        return Table.Connection.ExecuteAsync(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteScalar{T}(string, IEnumerable{object?})"/>
    public IEnumerable<TScalar> ExecuteScalars<TScalar>() {
        return Table.Connection.ExecuteScalars<TScalar>(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteScalarsAsync{T}(string, IDictionary{string, object?})"/>
    public Task<IEnumerable<TScalar>> ExecuteScalarsAsync<TScalar>() {
        return Table.Connection.ExecuteScalarsAsync<TScalar>(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteTable{T}.ExecuteQuery(string, IEnumerable{object?})"/>
    public IEnumerable<T> ExecuteQuery() {
        return Table.ExecuteQuery(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteTable{T}.ExecuteQueryAsync(string, IEnumerable{object?})"/>
    public IAsyncEnumerable<T> ExecuteQueryAsync() {
        return Table.ExecuteQueryAsync(GetCommand(), Parameters);
    }

    public SqlBuilder<T> Select(Expression<Func<T, object?>> column) {
        Select(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder<T> Select(Expression<Func<T, object?>> column, SelectType selectType) {
        Select(MemberExpressionToColumnName(column), selectType);
        return this;
    }
    public SqlBuilder<T> OrderBy(Expression<Func<T, object?>> column) {
        OrderBy(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder<T> OrderByDescending(Expression<Func<T, object?>> column) {
        OrderByDescending(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder<T> GroupBy(Expression<Func<T, object?>> column) {
        GroupBy(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder<T> Where(Expression<Func<T, bool>> predicate) {
        Where(ExpressionToSql(predicate.Body, predicate.Parameters[0]));
        return this;
    }
    public SqlBuilder<T> Having(Expression<Func<T, bool>> predicate) {
        Having(ExpressionToSql(predicate.Body, predicate.Parameters[0]));
        return this;
    }
    public SqlBuilder<T> Update(Expression<Func<T, object?>> column, Expression<Func<T, object?>> newValueExpression) {
        Update(MemberExpressionToColumnName(column), ExpressionToSql(newValueExpression.Body, newValueExpression.Parameters[0]));
        return this;
    }
    public SqlBuilder<T> Insert(Expression<Func<T, object?>> column, Expression<Func<T, object?>> valueExpression) {
        Insert(MemberExpressionToColumnName(column), ExpressionToSql(valueExpression.Body, valueExpression.Parameters[0]));
        return this;
    }

    public static string OperatorToSql(ExpressionType operatorType) => operatorType switch {
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
    public static string StringComparisonToCollation(StringComparison stringComparison) => stringComparison switch {
        StringComparison.Ordinal => Collation.Binary,
        StringComparison.OrdinalIgnoreCase => Collation.NoCase,
        StringComparison.InvariantCulture
        or StringComparison.CurrentCulture => Collation.Invariant,
        StringComparison.InvariantCultureIgnoreCase
        or StringComparison.CurrentCultureIgnoreCase => Collation.Invariant_NoCase,
        _ => throw new NotImplementedException($"{stringComparison.GetType()}")
    };

    public string GenerateParameterName() {
        CurrentParameterIndex++;
        return $"@p{CurrentParameterIndex}";
    }
    public string AddParameter(object? value) {
        string name = GenerateParameterName();
        Parameters.Add(name, value);
        return name;
    }

    private string MemberExpressionToColumnName(LambdaExpression expression) {
        if (expression.Body is not MemberExpression memberExpression) {
            throw new ArgumentException("Expected member expression");
        }
        return Table.MemberNameToColumnName(memberExpression.Member.Name);
    }
    private string ExpressionToSql(Expression expression, ParameterExpression rowExpression) {
        switch (expression) {
            // Constant (3)
            case ConstantExpression constantExpression:
                return AddParameter(constantExpression.Value);

            // Default (null)
            case DefaultExpression defaultExpression:
                return AddParameter(defaultExpression.Execute());

            // Unary (!a)
            case UnaryExpression unaryExpression:
                return $"({OperatorToSql(unaryExpression.NodeType)} {ExpressionToSql(unaryExpression.Operand, rowExpression)})";

            // Binary (a == b)
            case BinaryExpression binaryExpression:
                if (TryConvertEqualsNullToIsNull(binaryExpression, rowExpression, out string? isNullSql)) {
                    return isNullSql;
                }
                return $"({ExpressionToSql(binaryExpression.Left, rowExpression)} {OperatorToSql(binaryExpression.NodeType)} {ExpressionToSql(binaryExpression.Right, rowExpression)})";

            // Method Call (a.b())
            case MethodCallExpression methodCallExpression:
                if (TryConvertMethodCallToSql(methodCallExpression, out string? methodSql)) {
                    return $"({methodSql})";
                }
                // Executing the method will fail if it references the row parameter.
                return AddParameter(methodCallExpression.Execute());

            // Condition (a ? b : c)
            case ConditionalExpression conditionalExpression:
                return $"iif({ExpressionToSql(conditionalExpression.Test, rowExpression)}, {ExpressionToSql(conditionalExpression.IfTrue, rowExpression)}, {ExpressionToSql(conditionalExpression.IfFalse, rowExpression)})";

            // Member
            case MemberExpression memberExpression:
                if (TryConvertNonColumnMemberToSql(memberExpression, rowExpression, out string? memberSql)) {
                    return memberSql;
                }
                string columnName = Table.MemberNameToColumnName(memberExpression.Member.Name);
                return $"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}";

            // Not Supported
            default:
                throw new NotSupportedException($"{expression.GetType()}");
        }
    }
    /// <summary>
    /// Converts (a == null) to "a is null" because "null = null" is false.
    /// </summary>
    private bool TryConvertEqualsNullToIsNull(BinaryExpression binaryExpression, ParameterExpression rowExpression, [NotNullWhen(true)] out string? result) {
        result = null;

        if (binaryExpression.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual)) {
            return false;
        }

        Expression nonNullExpression;
        if (binaryExpression.Left.IsConstantNull()) {
            nonNullExpression = binaryExpression.Right;
        }
        else if (binaryExpression.Right.IsConstantNull()) {
            nonNullExpression = binaryExpression.Left;
        }
        else {
            return false;
        }

        result = $"{ExpressionToSql(nonNullExpression, rowExpression)} is {(binaryExpression.NodeType is ExpressionType.NotEqual ? "not" : "")} null";
        return true;
    }
    private bool TryConvertNonColumnMemberToSql(MemberExpression memberExpression, ParameterExpression rowExpression, [NotNullWhen(true)] out string? result) {
        // Member is a column of mapped row
        if (memberExpression.Expression == rowExpression) {
            result = null;
            return false;
        }
        // Member is unrelated to mapped row
        result = AddParameter(memberExpression.Execute());
        return true;
    }
    private bool TryConvertMethodCallToSql(MethodCallExpression methodCallExpression, [NotNullWhen(true)] out string? result) {
        // Found method to SQL converter
        if (MethodToSqlConverters.TryGetValue(methodCallExpression.Method, out MethodToSqlConverter<T>? methodToSqlConverter)) {
            result = methodToSqlConverter.Invoke(methodCallExpression);
            return true;
        }
        // Method call not recognised
        result = null;
        return false;
    }
    private void AddDefaultMethodToSqlConverters() {
        // string.Equals(string, string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(string)])!, methodCall => {
            string? str1 = (string?)methodCall.Arguments[0].Execute();
            string? str2 = (string?)methodCall.Arguments[1].Execute();

            return $"{AddParameter(str1)} = {AddParameter(str2)}";
        });

        // string.Equals(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string)])!, methodCall => {
            string? str1 = (string?)methodCall.Object.Execute();
            string? str2 = (string?)methodCall.Arguments[0].Execute();

            return $"{AddParameter(str1)} = {AddParameter(str2)}";
        });

        // string.Equals(string, string, StringComparison)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(string), typeof(StringComparison)])!, methodCall => {
            string? str1 = (string?)methodCall.Arguments[0].Execute();
            string? str2 = (string?)methodCall.Arguments[1].Execute();
            StringComparison strComparison = (StringComparison)methodCall.Arguments[2].Execute()!;

            return $"{AddParameter(str1)} = {AddParameter(str2)} collate {StringComparisonToCollation(strComparison).SqlQuote()}";
        });

        // string.Equals(string, StringComparison)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(StringComparison)])!, methodCall => {
            string? str1 = (string?)methodCall.Object.Execute();
            string? str2 = (string?)methodCall.Arguments[0].Execute();
            StringComparison strComparison = (StringComparison)methodCall.Arguments[1].Execute()!;

            return $"{AddParameter(str1)} = {AddParameter(str2)} collate {StringComparisonToCollation(strComparison).SqlQuote()}";
        });

        // string.Contains(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!, methodCall => {
            string? str1 = (string?)methodCall.Object.Execute();
            string? str2 = (string?)methodCall.Arguments[0].Execute();

            return $"{AddParameter(str1)} like {AddParameter("%" + str2 + "%")} escape '\\'";
        });

        // string.StartsWith(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!, methodCall => {
            string? str1 = (string?)methodCall.Object.Execute();
            string? str2 = (string?)methodCall.Arguments[0].Execute();

            return $"{AddParameter(str1)} like {AddParameter(str2 + "%")} escape '\\'";
        });

        // string.EndsWith(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!, methodCall => {
            string? str1 = (string?)methodCall.Object.Execute();
            string? str2 = (string?)methodCall.Arguments[0].Execute();

            return $"{AddParameter(str1)} like {AddParameter("%" + str2)} escape '\\'";
        });

        // string.ToLower()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.ToLower), [])!, methodCall => {
            string? str = (string?)methodCall.Object.Execute();

            return $"lower({AddParameter(str)})";
        });

        // string.ToUpper()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.ToUpper), [])!, methodCall => {
            string? str = (string?)methodCall.Object.Execute();

            return $"upper({AddParameter(str)})";
        });

        // string.IsNullOrEmpty(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.IsNullOrEmpty), [])!, methodCall => {
            string? str = (string?)methodCall.Object.Execute();
            string parameter = AddParameter(str);

            return $"({parameter} is null or {parameter} = '')";
        });

        // string.Trim()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Trim), [])!, methodCall => {
            string? str = (string?)methodCall.Object.Execute();

            return $"trim({AddParameter(str)})";
        });

        // string.TrimStart()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.TrimStart), [])!, methodCall => {
            string? str = (string?)methodCall.Object.Execute();

            return $"ltrim({AddParameter(str)})";
        });

        // string.TrimEnd()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.TrimEnd), [])!, methodCall => {
            string? str = (string?)methodCall.Object.Execute();

            return $"rtrim({AddParameter(str)})";
        });
    }
}

/// <summary>
/// A method that converts a <see cref="MethodCallExpression"/> to raw SQL.<br/>
/// To create parameters, use <see cref="SqlBuilder{T}.AddParameter(object?)"/>.
/// </summary>
public delegate string MethodToSqlConverter<T>(MethodCallExpression methodCall) where T : notnull, new();

/// <summary>
/// SQL aggregate functions (e.g. <c>SELECT COUNT(*)</c>)<br/>
/// See <see href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</see>.
/// </summary>
public enum SelectType {
    /// <summary>
    /// The mean (average) of the values.
    /// </summary>
    Avg,
    /// <summary>
    /// The number of non-null values.
    /// </summary>
    Count,
    /// <summary>
    /// The string concatenation of the non-null values.
    /// </summary>
    Group_Concat,
    /// <summary>
    /// The minimum non-null value.
    /// </summary>
    Min,
    /// <summary>
    /// The maximum non-null value.
    /// </summary>
    Max,
    /// <summary>
    /// The sum (addition) of all the non-null values.
    /// </summary>
    Sum,
    /// <summary>
    /// Similar to <see cref="Sum"/> but always returns a floating-point value (even if there are only integers or null).
    /// </summary>
    Total,
}