using System.Text;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace SQLiteSharp;

/// <summary>
/// A SQLite command builder for a table using the fluent style.<br/>
/// </summary>
/// <remarks>
/// Do not reuse.
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
    public Dictionary<MethodInfo, Func<MethodCallExpression, ParameterExpression, string>> MethodToSqlConverters { get; }
    /// <summary>
    /// Functions to convert CLR properties/fields to SQL expressions.
    /// </summary>
    public Dictionary<MemberInfo, Func<MemberExpression, ParameterExpression, string>> MemberToSqlConverters { get; }

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

    /// <summary>
    /// Constructs a <see cref="SqlBuilder{T}"/> to build and execute a complex SQL query using the fluent style.
    /// </summary>
    public SqlBuilder(SqliteTable<T> table) {
        Table = table;

        MethodToSqlConverters = GetDefaultMethodToSqlConverters();
        MemberToSqlConverters = GetDefaultMemberToSqlConverters();
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
        SelectList.Add($"{selectType.ToEnumString()}(*)");
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
        SelectList.Add($"{selectType.ToEnumString()}({Table.Name.SqlQuote()}.{columnName.SqlQuote()})");
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
        UpdateList.Add($"{columnName.SqlQuote()}", newValueExpression);
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
            builder.AppendLine($"update {Table.Name.SqlQuote()}");
            builder.AppendLine($"set {string.Join(",", UpdateList.Select(update => $"{update.Key} = {update.Value}"))}");
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
    /// <inheritdoc cref="SqliteConnection.ExecuteAsync(string, IReadOnlyDictionary{string, object?})"/>
    public Task<int> ExecuteAsync() {
        return Table.Connection.ExecuteAsync(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteScalars{T}(string, IEnumerable{object?})"/>
    public IEnumerable<TScalar> ExecuteScalars<TScalar>() {
        return Table.Connection.ExecuteScalars<TScalar>(GetCommand(), Parameters);
    }
    /// <inheritdoc cref="SqliteConnection.ExecuteScalarsAsync{T}(string, IReadOnlyDictionary{string, object?})"/>
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
        ExpressionType.Add
        or ExpressionType.AddChecked => "+",
        ExpressionType.Subtract
        or ExpressionType.SubtractChecked => "-",
        ExpressionType.Multiply
        or ExpressionType.MultiplyChecked => "*",
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
        _ => throw new NotImplementedException($"{stringComparison}")
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
        Expression body = expression.Body;
        // Unwrap type cast
        if (body is UnaryExpression unaryExpression && unaryExpression.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked) {
            body = unaryExpression.Operand;
        }
        // Ensure body is member expression
        if (body is not MemberExpression memberExpression) {
            throw new ArgumentException($"Expected MemberExpression, got '{expression.Body.GetType()}'");
        }
        // Get column name from member name
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
    private Dictionary<MethodInfo, Func<MethodCallExpression, ParameterExpression, string>> GetDefaultMethodToSqlConverters() => new() {
        // string.Equals(string, string)
        [typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(string)])!] = (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"{str1Sql} = {str2Sql}";
        },

        // string.Equals(string)
        [typeof(string).GetMethod(nameof(string.Equals), [typeof(string)])!] = (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{str1Sql} = {str2Sql}";
        },

        // string.Equals(string, string, StringComparison)
        [typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(string), typeof(StringComparison)])!] = (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            StringComparison strComparison = (StringComparison)methodCall.Arguments[2].Execute()!;
            return $"{str1Sql} = {str2Sql} collate {StringComparisonToCollation(strComparison).SqlQuote()}";
        },

        // string.Equals(string, StringComparison)
        [typeof(string).GetMethod(nameof(string.Equals), [typeof(string), typeof(StringComparison)])!] = (methodCall, rowExpression) => {
            string str1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            StringComparison strComparison = (StringComparison)methodCall.Arguments[1].Execute()!;
            return $"{str1Sql} = {str2Sql} collate {StringComparisonToCollation(strComparison).SqlQuote()}";
        },

        // string.Contains(string)
        [typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string? subStr = (string?)methodCall.Arguments[0].Execute();
            return $"{strSql} like {AddParameter("%" + subStr + "%")} escape '\\'";
        },

        // string.StartsWith(string)
        [typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string? subStr = (string?)methodCall.Arguments[0].Execute();
            return $"{strSql} like {AddParameter(subStr + "%")} escape '\\'";
        },

        // string.EndsWith(string)
        [typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string? subStr = (string?)methodCall.Arguments[0].Execute();
            return $"{strSql} like {AddParameter("%" + subStr)} escape '\\'";
        },

        // string.Replace(string, string)
        [typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string oldSubStrSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string newSubStrSql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"replace({strSql}, {oldSubStrSql}, {newSubStrSql})";
        },

        // string.Substring(int, int)
        [typeof(string).GetMethod(nameof(string.Substring), [typeof(int), typeof(int)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string startIndexSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string lengthSql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"substr({strSql}, {startIndexSql}, {lengthSql})";
        },

        // string.Substring(int)
        [typeof(string).GetMethod(nameof(string.Substring), [typeof(int)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            string startIndexSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"substr({strSql}, {startIndexSql})";
        },

        // string.ToLower()
        [typeof(string).GetMethod(nameof(string.ToLower), [])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"lower({strSql})";
        },

        // string.ToUpper()
        [typeof(string).GetMethod(nameof(string.ToUpper), [])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"upper({strSql})";
        },

        // string.IsNullOrEmpty(string)
        [typeof(string).GetMethod(nameof(string.IsNullOrEmpty), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{strSql} is null or {strSql} = ''";
        },

        // string.Trim()
        [typeof(string).GetMethod(nameof(string.Trim), [])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"trim({strSql})";
        },

        // string.TrimStart()
        [typeof(string).GetMethod(nameof(string.TrimStart), [])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"ltrim({strSql})";
        },

        // string.TrimEnd()
        [typeof(string).GetMethod(nameof(string.TrimEnd), [])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"rtrim({strSql})";
        },

        // string.Concat(string, string)
        [typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])!] = (methodCall, rowExpression) => {
            string str0Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str1Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"{str0Sql} || {str1Sql}";
        },

        // string.Concat(string, string, string)
        [typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string), typeof(string)])!] = (methodCall, rowExpression) => {
            string str0Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str1Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[2], rowExpression);
            return $"{str0Sql} || {str1Sql} || {str2Sql}";
        },

        // string.Concat(string, string, string, string)
        [typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string), typeof(string), typeof(string)])!] = (methodCall, rowExpression) => {
            string str0Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string str1Sql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            string str2Sql = ExpressionToSql(methodCall.Arguments[2], rowExpression);
            string str3Sql = ExpressionToSql(methodCall.Arguments[3], rowExpression);
            return $"{str0Sql} || {str1Sql} || {str2Sql} || {str3Sql})";
        },

        // Math.Abs(double)
        [typeof(Math).GetMethod(nameof(Math.Abs), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"abs({valueSql})";
        },

        // Math.Round(double)
        [typeof(Math).GetMethod(nameof(Math.Round), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"round({valueSql})";
        },

        // Math.Ceiling(double)
        [typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"ceil({valueSql})";
        },

        // Math.Floor(double)
        [typeof(Math).GetMethod(nameof(Math.Floor), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"floor({valueSql})";
        },

        // Math.Exp(double)
        [typeof(Math).GetMethod(nameof(Math.Exp), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"exp({valueSql})";
        },

        // Math.Log(double)
        [typeof(Math).GetMethod(nameof(Math.Log), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"log({valueSql})";
        },

        // Math.Pow(double, double)
        [typeof(Math).GetMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            string exponentSql = ExpressionToSql(methodCall.Arguments[1], rowExpression);
            return $"power({valueSql}, {exponentSql})";
        },

        // Math.Sqrt(double)
        [typeof(Math).GetMethod(nameof(Math.Sqrt), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"sqrt({valueSql})";
        },

        // Math.Sin(double)
        [typeof(Math).GetMethod(nameof(Math.Sin), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"sin({valueSql})";
        },

        // Math.Cos(double)
        [typeof(Math).GetMethod(nameof(Math.Cos), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"cos({valueSql})";
        },

        // Math.Tan(double)
        [typeof(Math).GetMethod(nameof(Math.Tan), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"tan({valueSql})";
        },

        // Math.Asin(double)
        [typeof(Math).GetMethod(nameof(Math.Asin), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"asin({valueSql})";
        },

        // Math.Acos(double)
        [typeof(Math).GetMethod(nameof(Math.Acos), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"acos({valueSql})";
        },

        // Math.Atan(double)
        [typeof(Math).GetMethod(nameof(Math.Atan), [typeof(double)])!] = (methodCall, rowExpression) => {
            string valueSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"atan({valueSql})";
        },

        // int.ToString()
        [typeof(int).GetMethod(nameof(int.ToString), [])!] = (methodCall, rowExpression) => {
            string intSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"cast({intSql} as text)";
        },

        // int.Parse(string)
        [typeof(int).GetMethod(nameof(int.Parse), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"cast({strSql} as integer)";
        },

        // int.Equals(int)
        [typeof(int).GetMethod(nameof(int.Equals), [typeof(int)])!] = (methodCall, rowExpression) => {
            string int1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string int2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{int1Sql} = {int2Sql}";
        },

        // long.ToString()
        [typeof(long).GetMethod(nameof(long.ToString), [])!] = (methodCall, rowExpression) => {
            string longSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"cast({longSql} as text)";
        },

        // long.Parse(string)
        [typeof(long).GetMethod(nameof(long.Parse), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"cast({strSql} as integer)";
        },

        // long.Equals(long)
        [typeof(long).GetMethod(nameof(long.Equals), [typeof(long)])!] = (methodCall, rowExpression) => {
            string long1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string long2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{long1Sql} = {long2Sql}";
        },

        // float.ToString()
        [typeof(float).GetMethod(nameof(float.ToString), [])!] = (methodCall, rowExpression) => {
            string floatSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"cast({floatSql} as text)";
        },

        // float.Parse(string)
        [typeof(float).GetMethod(nameof(float.Parse), [typeof(string)])!] = (methodCall, rowExpression) => {
            string strSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"cast({strSql} as integer)";
        },

        // float.Equals(float)
        [typeof(float).GetMethod(nameof(float.Equals), [typeof(float)])!] = (methodCall, rowExpression) => {
            string float1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string float2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{float1Sql} = {float2Sql}";
        },

        // double.ToString()
        [typeof(double).GetMethod(nameof(double.ToString), [])!] = (methodCall, rowExpression) => {
            string doubleSql = ExpressionToSql(methodCall.Object!, rowExpression);
            return $"cast({doubleSql} as text)";
        },

        // double.Parse(string)
        [typeof(double).GetMethod(nameof(double.Parse), [typeof(string)])!] = (methodCall, rowExpression) => {
            string doubleSql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"cast({doubleSql} as integer)";
        },

        // double.Equals(double)
        [typeof(double).GetMethod(nameof(double.Equals), [typeof(double)])!] = (methodCall, rowExpression) => {
            string double1Sql = ExpressionToSql(methodCall.Object!, rowExpression);
            string double2Sql = ExpressionToSql(methodCall.Arguments[0], rowExpression);
            return $"{double1Sql} = {double2Sql}";
        },
    };
    private Dictionary<MemberInfo, Func<MemberExpression, ParameterExpression, string>> GetDefaultMemberToSqlConverters() => new() {
        // string.Length
        [typeof(string).GetProperty(nameof(string.Length))!] = (member, rowExpression) => {
            string strSql = ExpressionToSql(member.Expression!, rowExpression);
            return $"length({strSql})";
        },
    };
}

/// <summary>
/// SQL aggregate functions (e.g. <c>SELECT COUNT(*)</c>)<br/>
/// See <see href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</see>.
/// </summary>
public enum SelectType {
    /// <summary>
    /// The mean (average) of the values.
    /// </summary>
    [EnumMember(Value = "avg")]
    Average,
    /// <summary>
    /// The number of non-null values.
    /// </summary>
    [EnumMember(Value = "count")]
    Count,
    /// <summary>
    /// The string concatenation of the non-null values.
    /// </summary>
    [EnumMember(Value = "group_concat")]
    GroupConcat,
    /// <summary>
    /// The minimum non-null value.
    /// </summary>
    [EnumMember(Value = "min")]
    Min,
    /// <summary>
    /// The maximum non-null value.
    /// </summary>
    [EnumMember(Value = "max")]
    Max,
    /// <summary>
    /// The sum (addition) of all the non-null values.
    /// </summary>
    [EnumMember(Value = "sum")]
    Sum,
    /// <summary>
    /// Similar to <see cref="Sum"/> but always returns a floating-point value (even if there are only integers or null).
    /// </summary>
    [EnumMember(Value = "total")]
    Total,
}