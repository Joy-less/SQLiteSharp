using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLiteSharp;

/// <summary>
/// A SQLite command builder for a table using the fluent style.<br/>
/// </summary>
/// <remarks>
/// Cannot be re-used.
/// </remarks>
public class SqlBuilder<T> where T : notnull, new() {
    /// <summary>
    /// The table the builder is building a command for.
    /// </summary>
    public SqliteTable<T> Table { get; }
    /// <summary>
    /// The current parameters to be used with the command.
    /// </summary>
    public Dictionary<string, object?> Parameters { get; } = [];
    /// <summary>
    /// Functions to convert CLR methods to SQL expressions.
    /// </summary>
    public Dictionary<MethodInfo, Func<MethodCallExpression, ParameterExpression, string>> MethodToSqlConverters { get; } = [];
    /// <summary>
    /// Functions to convert CLR properties/fields to SQL expressions.
    /// </summary>
    public Dictionary<MemberInfo, Func<MemberExpression, ParameterExpression, string>> MemberToSqlConverters { get; } = [];

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

    private int ParameterCounter;

    internal SqlBuilder(SqliteTable<T> table) {
        Table = table;

        AddDefaultSqlConverters();
    }

    /// <summary>
    /// Adds a <c>select</c> statement for every column.
    /// </summary>
    public SqlBuilder<T> Select() {
        SelectList.Add($"{Table.Name.SqlQuote()}.*");
        return this;
    }
    /// <summary>
    /// Adds a <c>select(...)</c> statement for every column.
    /// </summary>
    public SqlBuilder<T> Select(SelectType selectType) {
        SelectList.Add($"{selectType}(*)");
        return this;
    }
    /// <summary>
    /// Adds a <c>select</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> Select(string columnName) {
        SelectList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    /// <summary>
    /// Adds a <c>select(...)</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> Select(string columnName, SelectType selectType) {
        SelectList.Add($"{selectType}({Table.Name.SqlQuote()}.{columnName.SqlQuote()})");
        return this;
    }
    /// <summary>
    /// Adds an <c>order by asc</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> OrderBy(string columnName) {
        OrderByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} asc");
        return this;
    }
    /// <summary>
    /// Adds an <c>order by desc</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> OrderByDescending(string columnName) {
        OrderByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()} desc");
        return this;
    }
    /// <summary>
    /// Adds a <c>group by</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> GroupBy(string columnName) {
        GroupByList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}");
        return this;
    }
    /// <summary>
    /// Adds a <c>where</c> statement.
    /// </summary>
    public SqlBuilder<T> Where(string condition) {
        WhereList.Add(condition);
        return this;
    }
    /// <summary>
    /// Adds a <c>having</c> statement.
    /// </summary>
    /// <remarks>
    /// This is similar to <see cref="Where(string)"/> but applies after <see cref="GroupBy(string)"/>.
    /// </remarks>
    public SqlBuilder<T> Having(string condition) {
        HavingList.Add(condition);
        return this;
    }
    /// <summary>
    /// Adds a <c>limit</c> statement.
    /// </summary>
    public SqlBuilder<T> Take(long count) {
        LimitCount += count;
        return this;
    }
    /// <summary>
    /// Adds an <c>offset</c> statement.
    /// </summary>
    public SqlBuilder<T> Skip(long count) {
        OffsetCount += count;
        return this;
    }
    /// <summary>
    /// Adds an <c>update</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> Update(string columnName, string newValueExpression) {
        UpdateList.Add($"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}", newValueExpression);
        return this;
    }
    /// <summary>
    /// Adds an <c>insert</c> statement for a specific column.
    /// </summary>
    public SqlBuilder<T> Insert(string columnName, string valueExpression) {
        InsertList.Add($"{columnName.SqlQuote()}", valueExpression);
        return this;
    }
    /// <summary>
    /// Adds a <c>delete</c> statement.
    /// </summary>
    public SqlBuilder<T> Delete() {
        DeleteFlag = true;
        return this;
    }

    /// <summary>
    /// Builds a SQL command from the state of the builder, to be used with <see cref="Parameters"/>.
    /// </summary>
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
                builder.AppendLine($"where {string.Join(" and ", WhereList)}");
            }
            if (HavingList.Count > 0) {
                builder.AppendLine($"having {string.Join(" and ", HavingList)}");
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
                builder.AppendLine($"where {string.Join(" and ", WhereList)}");
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
            builder.AppendLine($"insert into {Table.Name.SqlQuote()}");
            if (InsertList.Count == 0) {
                builder.AppendLine("default values");
            }
            else {
                builder.AppendLine($"({string.Join(",", InsertList.Select(insert => insert.Key))})");
                builder.AppendLine($"values ({string.Join(",", InsertList.Select(insert => insert.Value))})");
            }
            if (WhereList.Count > 0) {
                builder.AppendLine($"where {string.Join(" and ", WhereList)}");
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
                builder.AppendLine($"where {string.Join(" and ", WhereList)}");
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
    /// <inheritdoc cref="SqliteConnection.ExecuteScalars{T}(string, IEnumerable{object?})"/>
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

    /// <inheritdoc cref="Select(string)"/>
    public SqlBuilder<T> Select(Expression<Func<T, object?>> column) {
        Select(MemberExpressionToColumnName(column));
        return this;
    }
    /// <inheritdoc cref="Select(string, SelectType)"/>
    public SqlBuilder<T> Select(Expression<Func<T, object?>> column, SelectType selectType) {
        Select(MemberExpressionToColumnName(column), selectType);
        return this;
    }
    /// <inheritdoc cref="OrderBy(string)"/>
    public SqlBuilder<T> OrderBy(Expression<Func<T, object?>> column) {
        OrderBy(MemberExpressionToColumnName(column));
        return this;
    }
    /// <inheritdoc cref="OrderByDescending(string)"/>
    public SqlBuilder<T> OrderByDescending(Expression<Func<T, object?>> column) {
        OrderByDescending(MemberExpressionToColumnName(column));
        return this;
    }
    /// <inheritdoc cref="GroupBy(string)"/>
    public SqlBuilder<T> GroupBy(Expression<Func<T, object?>> column) {
        GroupBy(MemberExpressionToColumnName(column));
        return this;
    }
    /// <inheritdoc cref="Where(string)"/>
    public SqlBuilder<T> Where(Expression<Func<T, bool>> predicate) {
        Where(ExpressionToSql(predicate.Body, predicate.Parameters[0]));
        return this;
    }
    /// <inheritdoc cref="Having(string)"/>
    public SqlBuilder<T> Having(Expression<Func<T, bool>> predicate) {
        Having(ExpressionToSql(predicate.Body, predicate.Parameters[0]));
        return this;
    }
    /// <inheritdoc cref="Update(string, string)"/>
    public SqlBuilder<T> Update(Expression<Func<T, object?>> column, Expression<Func<T, object?>> newValueExpression) {
        Update(MemberExpressionToColumnName(column), ExpressionToSql(newValueExpression.Body, newValueExpression.Parameters[0]));
        return this;
    }
    /// <inheritdoc cref="Update(Expression{Func{T, object?}}, Expression{Func{T, object?}})"/>
    public SqlBuilder<T> Update(Expression<Func<T, object?>> column, object? newValue) {
        Update(column, (T row) => newValue);
        return this;
    }
    /// <inheritdoc cref="Insert(string, string)"/>
    public SqlBuilder<T> Insert(Expression<Func<T, object?>> column, object? value) {
        Insert(MemberExpressionToColumnName(column), AddParameter(value));
        return this;
    }

    /// <summary>
    /// Converts the CLR operator to a SQL operator.
    /// </summary>
    public static string OperatorToSql(ExpressionType operatorType) => operatorType switch {
        ExpressionType.Not => "not",
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
    /// <summary>
    /// Converts the string comparison type to a SQLite collation name.
    /// </summary>
    public static string StringComparisonToCollation(StringComparison stringComparison) => stringComparison switch {
        StringComparison.Ordinal => Collation.Binary,
        StringComparison.OrdinalIgnoreCase => Collation.NoCase,
        StringComparison.InvariantCulture
        or StringComparison.CurrentCulture => Collation.Invariant,
        StringComparison.InvariantCultureIgnoreCase
        or StringComparison.CurrentCultureIgnoreCase => Collation.InvariantNoCase,
        _ => throw new NotImplementedException($"{stringComparison.GetType()}")
    };

    /// <summary>
    /// Increments the parameter counter and returns the formatted SQL parameter name (including the <c>@</c>).
    /// </summary>
    public string GenerateParameterName() {
        ParameterCounter++;
        return $"@p{ParameterCounter}";
    }
    /// <summary>
    /// Adds a parameter with the given value, returning the generated formatted SQL parameter name (including the <c>@</c>).
    /// </summary>
    public string AddParameter(object? value) {
        string name = GenerateParameterName();
        Parameters.Add(name, value);
        return name;
    }

    /// <summary>
    /// Converts a member (property/field) expression to the name of a column in the table.
    /// </summary>
    public string MemberExpressionToColumnName(LambdaExpression expression) {
        if (expression.Body is not MemberExpression memberExpression) {
            throw new ArgumentException($"Expected MemberExpression, got '{expression.Body.GetType()}'");
        }
        return Table.MemberNameToColumnName(memberExpression.Member.Name);
    }
    /// <summary>
    /// Recursively converts the CLR expression to a SQL expression, adding parameters where necessary.
    /// </summary>
    /// <param name="rowExpression">
    /// The row parameter (e.g. the <c>player</c> in <c>(player => player.name)</c>).
    /// </param>
    public string ExpressionToSql(Expression expression, ParameterExpression rowExpression) {
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
                if (binaryExpression.NodeType is ExpressionType.Coalesce) {
                    return $"coalesce({ExpressionToSql(binaryExpression.Left, rowExpression)}, {ExpressionToSql(binaryExpression.Right, rowExpression)})";
                }
                return $"({ExpressionToSql(binaryExpression.Left, rowExpression)} {OperatorToSql(binaryExpression.NodeType)} {ExpressionToSql(binaryExpression.Right, rowExpression)})";

            // Condition (a ? b : c)
            case ConditionalExpression conditionalExpression:
                return $"iif({ExpressionToSql(conditionalExpression.Test, rowExpression)}, {ExpressionToSql(conditionalExpression.IfTrue, rowExpression)}, {ExpressionToSql(conditionalExpression.IfFalse, rowExpression)})";

            // Method Call (a.b())
            case MethodCallExpression methodCallExpression:
                return $"({ConvertMethodCallToSql(methodCallExpression, rowExpression)})";

            // Member (a.b)
            case MemberExpression memberExpression:
                return $"({ConvertMemberToSql(memberExpression, rowExpression)})";

            // Not Supported
            default:
                throw new NotSupportedException($"{expression.GetType()}");
        }
    }

    /// <summary>
    /// Converts (a == null) to "a is null" because "null = null" is false.
    /// </summary>
    private bool TryConvertEqualsNullToIsNull(BinaryExpression binaryExpression, ParameterExpression rowExpression, [NotNullWhen(true)] out string? result) {
        if (binaryExpression.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual)) {
            result = null;
            return false;
        }

        Expression nonNullExpression;
        if (binaryExpression.Left is ConstantExpression { Value: null }) {
            nonNullExpression = binaryExpression.Right;
        }
        else if (binaryExpression.Right is ConstantExpression { Value: null }) {
            nonNullExpression = binaryExpression.Left;
        }
        else {
            result = null;
            return false;
        }

        result = $"{ExpressionToSql(nonNullExpression, rowExpression)} is {(binaryExpression.NodeType is ExpressionType.NotEqual ? "not" : "")} null";
        return true;
    }
    private string ConvertMemberToSql(MemberExpression memberExpression, ParameterExpression rowExpression) {
        // Member is column of row
        if (memberExpression.Expression == rowExpression) {
            string columnName = Table.MemberNameToColumnName(memberExpression.Member.Name);
            return $"{Table.Name.SqlQuote()}.{columnName.SqlQuote()}";
        }
        // Member has SQL converter
        if (MemberToSqlConverters.TryGetValue(memberExpression.Member, out Func<MemberExpression, ParameterExpression, string>? memberToSqlConverter)) {
            return memberToSqlConverter.Invoke(memberExpression, rowExpression);
        }
        // Member not recognised
        return AddParameter(memberExpression.Execute());
    }
    private string ConvertMethodCallToSql(MethodCallExpression methodCallExpression, ParameterExpression rowExpression) {
        // Method has SQL converter
        if (MethodToSqlConverters.TryGetValue(methodCallExpression.Method, out Func<MethodCallExpression, ParameterExpression, string>? methodToSqlConverter)) {
            return methodToSqlConverter.Invoke(methodCallExpression, rowExpression);
        }
        // Method call not recognised
        return AddParameter(methodCallExpression.Execute());
    }
    private void AddDefaultSqlConverters() {
        // string.Equals(string, string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(string)])!, (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"{str1Sql} = {str2Sql}";
        });

        // string.Equals(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string)])!, (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{str1Sql} = {str2Sql}";
        });

        // string.Equals(string, string, StringComparison)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(string), typeof(StringComparison)])!, (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            StringComparison strComparison = (StringComparison)methodCall.Arguments[2].Execute()!;
            return $"{str1Sql} = {str2Sql} collate {StringComparisonToCollation(strComparison).SqlQuote()}";
        });

        // string.Equals(string, StringComparison)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(StringComparison)])!, (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            StringComparison strComparison = (StringComparison)methodCall.Arguments[1].Execute()!;
            return $"{str1Sql} = {str2Sql} collate {StringComparisonToCollation(strComparison).SqlQuote()}";
        });

        // string.Contains(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string? subStr = (string?)methodCall.Arguments[0].Execute();
            return $"{strSql} like {AddParameter("%" + subStr + "%")} escape '\\'";
        });

        // string.StartsWith(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string? subStr = (string?)methodCall.Arguments[0].Execute();
            return $"{strSql} like {AddParameter(subStr + "%")} escape '\\'";
        });

        // string.EndsWith(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string? subStr = (string?)methodCall.Arguments[0].Execute();
            return $"{strSql} like {AddParameter("%" + subStr)} escape '\\'";
        });

        // string.Replace(string, string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string oldSubStrSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string newSubStrSql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"replace({strSql}, {oldSubStrSql}, {newSubStrSql})";
        });

        // string.Substring(int, int)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Substring), [typeof(int), typeof(int)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string startIndexSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string lengthSql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"substr({strSql}, {startIndexSql}, {lengthSql})";
        });

        // string.Substring(int)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Substring), [typeof(int)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string startIndexSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"substr({strSql}, {startIndexSql})";
        });

        // string.Length
        MemberToSqlConverters.Add(typeof(string).GetProperty(nameof(string.Length))!, (member, rowExpression) => {
            string strSql = ExpressionToSql(member.Expression!, rowExpression);
            return $"length({strSql})";
        });

        // string.ToLower()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.ToLower), [])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"lower({strSql})";
        });

        // string.ToUpper()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.ToUpper), [])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"upper({strSql})";
        });

        // string.IsNullOrEmpty(string)
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.IsNullOrEmpty), [typeof(string)])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"({strSql} is null or {strSql} = '')";
        });

        // string.Trim()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.Trim), [])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"trim({strSql})";
        });

        // string.TrimStart()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.TrimStart), [])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"ltrim({strSql})";
        });

        // string.TrimEnd()
        MethodToSqlConverters.Add(typeof(string).GetMethod(nameof(string.TrimEnd), [])!, (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"rtrim({strSql})";
        });

        // Math.Abs(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Abs), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"abs({valueSql})";
        });

        // Math.Round(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Round), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"round({valueSql})";
        });

        // Math.Ceiling(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"ceil({valueSql})";
        });

        // Math.Floor(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Floor), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"floor({valueSql})";
        });

        // Math.Exp(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Exp), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"exp({valueSql})";
        });

        // Math.Log(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Log), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"log({valueSql})";
        });

        // Math.Pow(double, double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string exponentSql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"power({valueSql}, {exponentSql})";
        });

        // Math.Sqrt(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Sqrt), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"sqrt({valueSql})";
        });

        // Math.Sin(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Sin), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"sin({valueSql})";
        });

        // Math.Cos(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Cos), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"cos({valueSql})";
        });

        // Math.Tan(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Tan), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"tan({valueSql})";
        });

        // Math.Asin(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Asin), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"asin({valueSql})";
        });

        // Math.Acos(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Acos), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"acos({valueSql})";
        });

        // Math.Atan(double)
        MethodToSqlConverters.Add(typeof(Math).GetMethod(nameof(Math.Atan), [typeof(double)])!, (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"atan({valueSql})";
        });
    }
}

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