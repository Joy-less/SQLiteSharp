using System;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLiteSharp;

public class SqlBuilder2<T>(SqliteTable<T> table) where T : notnull, new() {
    public SqliteTable<T> Table { get; } = table;
    public Dictionary<string, object?> Parameters { get; } = [];
    public Dictionary<string, object?> Sql = [];

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
    public SqlBuilder2<T> Where(string condition) {
        WhereList.Add(condition);
        return this;
    }
    /*public SqlBuilder2<T> Where(string columnName, string @operator, object? value, bool negate = false) {
        Where($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} {@operator} {AddParameter(value)}");
        return this;
    }
    public SqlBuilder2<T> Where(string columnName, string @operator, bool negate = false) {
        Where($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} {@operator}");
        return this;
    }*/
    public SqlBuilder2<T> WhereIn(string columnName, IEnumerable values, bool negate = false) {
        IEnumerable<string> parameterNames = values.Cast<object?>().Select(AddParameter);
        Where($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} in ({string.Join(",", parameterNames)})");
        return this;
    }
    public SqlBuilder2<T> Having(string condition) {
        HavingList.Add(condition);
        return this;
    }
    /*public SqlBuilder2<T> Having(string columnName, string @operator, object? value, bool negate = false) {
        Having($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} {@operator} {AddParameter(value)}");
        return this;
    }
    public SqlBuilder2<T> Having(string columnName, string @operator, bool negate = false) {
        Having($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} {@operator}");
        return this;
    }*/
    public SqlBuilder2<T> HavingIn(string columnName, IEnumerable values, bool negate = false) {
        IEnumerable<string> parameterNames = values.Cast<object?>().Select(AddParameter);
        Having($"{(negate ? "not" : "")} {Table.Name.SqlQuote()}.{columnName.SqlQuote()} in ({string.Join(",", parameterNames)})");
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
    public SqlBuilder2<T> Update(string columnName, string newValueExpression) {
        UpdateList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", newValueExpression);
        return this;
    }
    public SqlBuilder2<T> Insert(string columnName, object? value) {
        InsertList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", AddParameter(value));
        return this;
    }
    public SqlBuilder2<T> Delete() {
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

    /// <inheritdoc cref="SqliteTable{T}.ExecuteQuery(string, IEnumerable{object?})"/>
    public IEnumerable<T> ExecuteQuery() {
        return Table.ExecuteQuery(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteTable{T}.ExecuteQueryAsync(string, IEnumerable{object?})"/>
    public IAsyncEnumerable<T> ExecuteQueryAsync() {
        return Table.ExecuteQueryAsync(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.Execute(string, IEnumerable{object?})"/>
    public int Execute() {
        return Table.Connection.Execute(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.Execute(string, IDictionary{string, object?})"/>
    public Task<int> ExecuteAsync() {
        return Table.Connection.ExecuteAsync(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteQueryScalars{T}(string, IEnumerable{object?})"/>
    public IEnumerable<TScalar> ExecuteQueryScalars<TScalar>() {
        return Table.Connection.ExecuteQueryScalars<TScalar>(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteQueryScalars{T}(string, IDictionary{string, object?})"/>
    public Task<IEnumerable<TScalar>> ExecuteQueryScalarsAsync<TScalar>() {
        return Table.Connection.ExecuteQueryScalarsAsync<TScalar>(GetCommand(), Parameters);
    }

    public SqlBuilder2<T> Select(Expression<Func<T, object?>> column) {
        Select(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder2<T> Select(Expression<Func<T, object?>> column, SelectType selectType) {
        Select(MemberExpressionToColumnName(column), selectType);
        return this;
    }
    public SqlBuilder2<T> OrderBy(Expression<Func<T, object?>> column) {
        OrderBy(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder2<T> OrderByDescending(Expression<Func<T, object?>> column) {
        OrderByDescending(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder2<T> GroupBy(Expression<Func<T, object?>> column) {
        GroupBy(MemberExpressionToColumnName(column));
        return this;
    }
    public SqlBuilder2<T> Where(Expression<Func<T, bool>> predicate) {
        Where(ExpressionToSql(predicate.Body, predicate.Parameters[0]));
        return this;
    }
    public SqlBuilder2<T> WhereIn(Expression<Func<T, bool>> predicate, IEnumerable values) {
        WhereIn(ExpressionToSql(predicate.Body, predicate.Parameters[0]), values);
        return this;
    }
    public SqlBuilder2<T> WhereNotIn(Expression<Func<T, bool>> predicate, IEnumerable values) {
        WhereIn(ExpressionToSql(predicate.Body, predicate.Parameters[0]), values, negate: true);
        return this;
    }
    public SqlBuilder2<T> Having(Expression<Func<T, bool>> predicate) {
        Having(ExpressionToSql(predicate.Body, predicate.Parameters[0]));
        return this;
    }
    public SqlBuilder2<T> HavingIn(Expression<Func<T, bool>> predicate, IEnumerable values) {
        HavingIn(ExpressionToSql(predicate.Body, predicate.Parameters[0]), values);
        return this;
    }
    public SqlBuilder2<T> HavingNotIn(Expression<Func<T, bool>> predicate, IEnumerable values) {
        HavingIn(ExpressionToSql(predicate.Body, predicate.Parameters[0]), values, negate: true);
        return this;
    }
    public SqlBuilder2<T> Update(Expression<Func<T, object?>> column, Expression<Func<T, object?>> newValueExpression) {
        Update(MemberExpressionToColumnName(column), ExpressionToSql(newValueExpression.Body, newValueExpression.Parameters[0]));
        return this;
    }
    public SqlBuilder2<T> Insert(Expression<Func<T, object?>> column, Expression<Func<T, object?>> valueExpression) {
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
    /*public static string LikeToSql(string expression, LikeMethod method) {
        return method switch {
            LikeMethod.Equals => $"{expression}",
            LikeMethod.StartsWith => $"{expression}%",
            LikeMethod.EndsWith => $"%{expression}",
            LikeMethod.Contains => $"%{expression}%",
            _ => throw new NotImplementedException($"{method}")
        };
    }*/

    public string GenerateParameterName() {
        CurrentParameterIndex++;
        return $"@p{CurrentParameterIndex}";
    }
    public string AddParameter(object? value) {
        string name = GenerateParameterName();
        Parameters.Add(name, value);
        return name;
    }

    private string ExpressionToSql(Expression expression, ParameterExpression rowExpression) {
        switch (expression) {
            // Constant (3)
            case ConstantExpression constantExpression:
                return AddParameter(constantExpression.Value);

            // Unary (!a)
            case UnaryExpression unaryExpression:
                return $"{OperatorToSql(unaryExpression.NodeType)} {ExpressionToSql(unaryExpression.Operand, rowExpression)}";

            // Binary (a == b)
            case BinaryExpression binaryExpression:
                if (TryConvertEqualsNullToIsNull(binaryExpression, rowExpression, out string? isNullSql)) {
                    return isNullSql;
                }
                return $"{ExpressionToSql(binaryExpression.Left, rowExpression)} {OperatorToSql(binaryExpression.NodeType)} {ExpressionToSql(binaryExpression.Right, rowExpression)}";

            // Method Call (a.b())
            case MethodCallExpression methodCallExpression:
                if (TryConvertMethodCallToSql(methodCallExpression, out string? methodSql)) {
                    return methodSql;
                }
                return AddParameter(methodCallExpression.Execute());

            // Condition (a ? b : c)
            case ConditionalExpression conditionalExpression:
                return $"iif({ExpressionToSql(conditionalExpression.Test, rowExpression)}, {ExpressionToSql(conditionalExpression.IfTrue, rowExpression)}, {ExpressionToSql(conditionalExpression.IfFalse, rowExpression)})";

            // Default (null)
            case DefaultExpression defaultExpression:
                return AddParameter(defaultExpression.Execute());

            // Member
            case MemberExpression memberExpression:
                if (TryConvertUnrelatedMemberToSql(memberExpression, rowExpression, out string? memberSql)) {
                    return memberSql;
                }
                string columnName = Table.MemberNameToColumnName(memberExpression.Member.Name);
                return $"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}";

            // Not Supported
            default:
                throw new NotSupportedException($"{expression.GetType()}");
        }
    }
    private string MemberExpressionToColumnName(LambdaExpression expression) {
        if (expression.Body is not MemberExpression memberExpression) {
            throw new ArgumentException("Expected member expression");
        }
        string columnName = Table.MemberNameToColumnName(memberExpression.Member.Name);
        return columnName;
    }
    /// <summary>
    /// Convert (a == null) to "a is null" because "null = null" is false.
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
    private bool TryConvertMethodCallToSql(MethodCallExpression methodCallExpression, [NotNullWhen(true)] out string? result) {
        result = null;
        return false;
    }
    private bool TryConvertUnrelatedMemberToSql(MemberExpression memberExpression, ParameterExpression rowExpression, [NotNullWhen(true)] out string? result) {
        if (memberExpression.Expression == rowExpression) {
            result = null;
            return false;
        }
        result = AddParameter(memberExpression.Execute());
        return true;
    }
}

/// <summary>
/// SQL aggregate functions (e.g. <c>SELECT COUNT(*)</c>)<br/>
/// See <see href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</see>.
/// </summary>
public enum SelectType {
    Avg,
    Count,
    Min,
    Max,
    Sum,
    Total,
}

/*public enum LikeMethod {
    StartsWith,
    EndsWith,
    Contains,
    Equals,
}*/

/*public abstract class UpdateResolver<T> where T : notnull, new() {
    public abstract MethodInfo Method { get; }
    public abstract Action<SqlBuilder2<T>, MethodCallExpression, object?[]> Resolve { get; }
}
public class StringReplaceResolver : UpdateResolver {
    public override MethodInfo Method {
        get => typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;
    }
    public override void Resolve(SqlBuilder builder, MethodCallExpression callExpression, object?[] arguments) {
        if (arguments.Length != 2) {
            throw new ArgumentException($"REPLACE query requires 2 arguments for replacing old_string with new_string");
        }
        string columnName = LambdaResolver.GetColumnName(callExpression.Object!);
        builder.UpdateColumnReplaceString(columnName, arguments[0], arguments[1]);
    }
}*/