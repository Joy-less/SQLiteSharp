using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace SQLiteSharp.LinqToSQL;

/// <summary>
/// Represents the basic operations / properties to generate the SQL queries.
/// </summary>
public abstract class SqlBuilder {
    public abstract string CommandText { get; }
    public abstract Dictionary<string, object?> CommandParameters { get; }

    public int CurrentParameterIndex { get; private set; }
    public List<UpdateResolver> UpdateResolvers { get; } = [new StringReplaceResolver()];

    private readonly List<string> UpdateValues = [];
    private readonly List<string> TableNames = [];
    private readonly List<string> JoinExpressions = [];
    private readonly List<string> SelectionList = [];
    private readonly List<string> WhereConditions = [];
    private readonly List<string> OrderByList = [];
    private readonly List<string> GroupByList = [];
    private readonly List<string> HavingConditions = [];
    private readonly List<string> SplitColumns = [];

    public string InsertTarget => Operation switch {
        SqlOperation.Insert => Adapter.Table(TableNames.First()),
        SqlOperation.InsertFrom => Adapter.Table(TableNames.Last()),
        _ => throw new NotSupportedException("The property is not supported in other queries than INSERT query statement"),
    };

    private string Source {
        get {
            string joinExpression = string.Join(" ", JoinExpressions);
            return $"{Adapter.Table(TableNames.First())} {joinExpression}";
        }
    }

    private string Selection {
        get {
            if (SelectionList.Count == 0) {
                if (JoinExpressions.Count == 0) {
                    return $"{Adapter.Table(TableNames.First())}.*";
                }

                IEnumerable<string> joinTables = TableNames.Select(_ => $"{Adapter.Table(_)}.*");

                string selection = string.Join(", ", joinTables);
                return selection;
            }
            return string.Join(", ", SelectionList);
        }
    }

    private string Conditions => WhereConditions.Count == 0 ? "" : "WHERE " + string.Join("", WhereConditions);
    private string UpdateValues => string.Join(", ", UpdateValues);
    private List<Dictionary<string, object?>> InsertValues { get; } = [];
    private string Order => OrderByList.Count == 0 ? "" : "ORDER BY " + string.Join(", ", OrderByList);
    private string Grouping => GroupByList.Count == 0 ? "" : "GROUP BY " + string.Join(", ", GroupByList);
    private string Having => HavingConditions.Count == 0 ? "" : "HAVING " + string.Join(" ", HavingConditions);

    public void Join(string originalTableName, string joinTableName, string leftField, string rightField) {
        string joinString = $"JOIN {joinTableName.SqlQuote()} ON {FormatSqlMember(originalTableName, leftField)} = {FormatSqlMember(joinTableName, rightField)}";

        TableNames.Add(joinTableName);
        JoinExpressions.Add(joinString);
        SplitColumns.Add(rightField);
    }
    public void OrderBy(string tableName, string columnName, bool descending = false) {
        string order = FormatSqlMember(tableName, columnName);
        if (descending) {
            order += " DESC";
        }
        OrderByList.Add(order);
    }
    public void Select(string tableName) {
        string selectionString = $"{tableName.SqlQuote()}.*";
        SelectionList.Add(selectionString);
    }
    public void Select(string tableName, string columnName) {
        SelectionList.Add(FormatSqlMember(tableName, columnName));
    }
    public void Select(string tableName, string columnName, SelectFunction selectFunction) {
        var selectionString = $"{selectFunction}({FormatSqlMember(tableName, columnName)})";
        SelectionList.Add(selectionString);
    }
    public void Select(SelectFunction selectFunction) {
        var selectionString = $"{selectFunction}(*)";
        SelectionList.Add(selectionString);
    }
    public void GroupBy(string tableName, string columnName) {
        GroupByList.Add(FormatSqlMember(tableName, columnName));
    }

    public void QueryByField(string tableName, string columnName, string op, object fieldValue) {
        WhereConditions.Add($"{FormatSqlMember(tableName, columnName)} {op} {AddParameter(fieldValue)}");
    }
    public void QueryByFieldLike(string tableName, string columnName, string fieldValue) {
        QueryByField(tableName, columnName, "LIKE", fieldValue);
    }
    public void QueryByFieldNull(string tableName, string columnName) {
        WhereConditions.Add($"{FormatSqlMember(tableName, columnName)} IS NULL");
    }
    public void QueryByFieldNotNull(string tableName, string columnName) {
        WhereConditions.Add($"{FormatSqlMember(tableName, columnName)} IS NOT NULL");
    }
    public void QueryByFieldComparison(string leftTableName, string leftFieldName, string op, string rightTableName, string rightFieldName) {
        WhereConditions.Add($"{FormatSqlMember(leftTableName, leftFieldName)} {op} {FormatSqlMember(rightTableName, rightFieldName)}");
    }
    public void QueryByIsIn(string tableName, string columnName, SqlBuilder sqlQuery) {
        var innerQuery = sqlQuery.CommandText;
        foreach (var param in sqlQuery.CommandParameters) {
            var innerParamKey = "Inner" + param.Key;
            innerQuery = Regex.Replace(innerQuery, param.Key, innerParamKey);
            CommandParameters.Add(innerParamKey, param.Value);
        }

        var newCondition = string.Format("{0} IN ({1})", Adapter.Field(tableName, columnName), innerQuery);

        WhereConditions.Add(newCondition);
    }
    public void QueryByIsIn(string tableName, string columnName, IEnumerable<object> parameters) {
        IEnumerable<string> parameterNames = parameters.Select(AddParameter);
        string newCondition = $"{FormatSqlMember(tableName, columnName)} IN ({string.Join(",", parameterNames)})";
        WhereConditions.Add(newCondition);
    }

    public static string FormatSqlMember(string parent, string child) {
        return $"{parent.SqlQuote()}.{child.SqlQuote()}";
    }
    public static string FormatSqlParameter(string parameter) {
        return $"@{parameter}";
    }
    public static string GetSqlOperator(ExpressionType operatorType) => operatorType switch {
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

    protected void AddAnd() {
        if (WhereConditions.Count > 0) {
            WhereConditions.Add(" AND ");
        }
    }
    protected void AddOr() {
        if (WhereConditions.Count > 0) {
            WhereConditions.Add(" OR ");
        }
    }
    protected void AddNot() {
        WhereConditions.Add(" NOT ");
    }
    protected void BuildSql(Node node) {
        if (node is LikeNode likeNode) {
            if (likeNode.Method is LikeMethod.Equals) {
                QueryByField(likeNode.MemberNode.TableName, likeNode.MemberNode.FieldName, GetSqlOperator(ExpressionType.Equal), likeNode.Value);
            }
            else {
                string? value = likeNode.Method switch {
                    LikeMethod.Equals => likeNode.Value,
                    LikeMethod.StartsWith => likeNode.Value + "%",
                    LikeMethod.EndsWith => "%" + likeNode.Value,
                    LikeMethod.Contains => "%" + likeNode.Value + "%",
                    _ => throw new NotImplementedException($"'{likeNode.Method}'")
                };
                QueryByFieldLike(likeNode.MemberNode.TableName, likeNode.MemberNode.FieldName, value);
            }
        }
        else if (node is OperationNode operationNode) {
            BuildSql(operationNode.Left, operationNode.Right, operationNode.Operator);
        }
        else if (node is MemberNode memberNode) {
            QueryByField(memberNode.TableName, memberNode.FieldName, GetSqlOperator(ExpressionType.Equal), true);
        }
        else if (node is SingleOperationNode singleOperationNode) {
            if (singleOperationNode.Operator is ExpressionType.Not) {
                AddNot();
            }
            BuildSql(singleOperationNode.Child);
        }
        else {
            throw new NotSupportedException($"{node.GetType()}");
        }
    }
    protected void BuildSql(Node leftNode, Node rightNode, ExpressionType operatorType) {
        WhereConditions.Add("(");
        BuildSql(leftNode);
        WhereConditions.Add(GetSqlOperator(operatorType));
        BuildSql(rightNode);
        WhereConditions.Add(")");
    }

    private string GenerateParameterName() {
        CurrentParameterIndex++;
        return FormatSqlParameter($"parameter{CurrentParameterIndex}");
    }
    private string AddParameter(object? value) {
        string name = GenerateParameterName();
        CommandParameters.Add(name, value);
        return name;
    }
}

/// <summary>
/// Represents the service that will generate SQL commands from given lambda expression.
/// </summary>
public class SqlBuilder<T>(SqliteTable<T> table) : SqlBuilder where T : notnull, new() {
    public SqliteTable<T> Table { get; set; } = table;

    public override string CommandText => Operation switch {
        SqlOperation.Insert => Adapter.InsertCommand(InsertTarget, InsertValues, _insertOutput),
        SqlOperation.InsertFrom => Adapter.InsertFromCommand(InsertTarget, Source, InsertValues, Conditions),
        SqlOperation.Update => Adapter.UpdateCommand(UpdateValues, Source, Conditions),
        SqlOperation.Delete => Adapter.DeleteCommand(Source, Conditions),
        _ => GenerateQueryCommand()
    };
    public override Dictionary<string, object?> CommandParameters { get; } = [];

    private SqlOperation Operation = SqlOperation.Query;

    private int? _pageSize;
    private int _pageIndex;

    private string _insertOutput = "";

    public SqlBuilder<T> Where(Expression<Func<T, bool>> expression) {
        AddAnd();
        ResolveQuery(expression);
        return this;
    }
    public SqlBuilder<T> WhereIn(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        AddAnd();
        QueryByIsIn(expression, sqlQuery);
        return this;
    }
    public SqlBuilder<T> WhereIn(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        AddAnd();
        QueryByIsIn(expression, values);
        return this;
    }
    public SqlBuilder<T> WhereNotIn(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        AddAnd();
        QueryByNotIn(expression, sqlQuery);
        return this;
    }
    public SqlBuilder<T> WhereNotIn(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        AddAnd();
        QueryByNotIn(expression, values);
        return this;
    }

    public SqlBuilder<T> OrderBy(Expression<Func<T, object>> expression) {
        OrderBy(expression);
        return this;
    }

    public SqlBuilder<T> Take(int pageSize) {
        _pageSize = pageSize;
        return this;
    }

    /// <summary>
    /// Use with <see cref="Take"/>(), to skip specified pages of result
    /// </summary>
    /// <param name="pageIndex">Number of pages to skip</param>
    public SqlBuilder<T> Skip(int pageIndex) {
        _pageIndex = pageIndex;
        return this;
    }

    public SqlBuilder<T> OrderByDescending(Expression<Func<T, object>> expression) {
        OrderBy(expression, true);
        return this;
    }

    public SqlBuilder<T> Select(params IEnumerable<Expression<Func<T, object?>>> expressions) {
        foreach (Expression<Func<T, object?>> expression in expressions) {
            Select(expression);
        }
        return this;
    }

    public SqlBuilder<T> Select<TResult>(Expression<Func<T, TResult>> expression) {
        Select(expression.Body);
        return this;
    }

    public SqlBuilder<T> Update(Expression<Func<T, object>> expression) {
        Update(expression.Body);
        return this;
    }

    /// <summary>
    /// Performs insert a new record from the given expression
    /// </summary>
    /// <param name="expression">The expression describes what to insert</param>
    /// <returns></returns>
    public SqlBuilder<T> Insert(Expression<Func<T, T>> expression) {
        Insert(expression);
        return this;
    }

    /// <summary>
    /// Append OUTPUT to the insert statement to get the output identity of the inserted record.
    /// </summary>
    public SqlBuilder<T> OutputInsertColumn() {
        OutputInsertColumn();
        return this;
    }

    /// <summary>
    /// Performs insert many records from the given expression
    /// </summary>
    /// <param name="expression">The expression describes the entities to insert</param>
    /// <returns></returns>
    public SqlBuilder<T> Insert(Expression<Func<T, IEnumerable<T>>> expression) {
        Insert(expression.Body);
        return this;
    }

    public SqlBuilder<T> SelectCount(Expression<Func<T, object>> expression) {
        SelectWithFunction(expression, SelectFunction.COUNT);
        return this;
    }
    public SqlBuilder<T> SelectCountAll() {
        SelectWithFunction<T>(SelectFunction.COUNT);
        return this;
    }
    public SqlBuilder<T> SelectDistinct(Expression<Func<T, object>> expression) {
        SelectWithFunction(expression, SelectFunction.DISTINCT);
        return this;
    }
    public SqlBuilder<T> SelectSum(Expression<Func<T, object>> expression) {
        SelectWithFunction(expression, SelectFunction.SUM);
        return this;
    }
    public SqlBuilder<T> SelectMax(Expression<Func<T, object>> expression) {
        SelectWithFunction(expression, SelectFunction.MAX);
        return this;
    }
    public SqlBuilder<T> SelectMin(Expression<Func<T, object>> expression) {
        SelectWithFunction(expression, SelectFunction.MIN);
        return this;
    }
    public SqlBuilder<T> SelectAverage(Expression<Func<T, object>> expression) {
        SelectWithFunction(expression, SelectFunction.AVG);
        return this;
    }

    public SqlBuilder<TResult> Join<T2, TKey, TResult>(SqlBuilder<T2> joinQuery, Expression<Func<T, TKey>> primaryKeySelector, Expression<Func<T, TKey>> foreignKeySelector) where T2 : notnull, new() where TResult : notnull, new() {
        SqlBuilder<TResult> query = new(Builder, Resolver);
        Join<T, T2, TKey>(primaryKeySelector, foreignKeySelector);
        return query;
    }
    public SqlBuilder<T2> Join<T2>(Expression<Func<T, T2, bool>> expression) where T2 : notnull, new() {
        SqlBuilder<T2> joinQuery = new(Builder, Resolver);
        Join(expression);
        return joinQuery;
    }

    public SqlBuilder<T> GroupBy(Expression<Func<T, object>> expression) {
        GroupBy(expression);
        return this;
    }

    private static string GetColumnName(MemberInfo memberInfo) {
        ColumnAttribute? columnAttribute = memberInfo.GetCustomAttribute<ColumnAttribute>();
        return (columnAttribute?.Name ?? memberInfo.Name).SqlQuote();
    }
    public static string GetColumnName(Expression<Func<T, object>> selector) {
        return GetColumnName(GetMemberExpression(selector.Body));
    }
    public static string GetColumnName(Expression expression) {
        return GetColumnName(GetMemberExpression(expression));
    }
    public static string GetColumnName(MemberAssignment expression) {
        return GetColumnName(expression.Member);
    }
    private static string GetColumnName(MemberExpression member) {
        return GetColumnName(member.Member);
    }

    public static string GetTableName(Type type) {
        TableAttribute? tableAttribute = type.GetCustomAttribute<TableAttribute>();
        return (tableAttribute?.Name ?? type.Name).SqlQuote();
    }
    public static string GetTableName<T2>() {
        return GetTableName(typeof(T2));
    }
    private static string GetTableName(MemberExpression expression) {
        return GetTableName(expression.Expression!.Type);
    }

    private static MemberExpression GetMemberExpression(Expression expression) {
        return expression.NodeType switch {
            ExpressionType.MemberAccess => (MemberExpression)expression,
            ExpressionType.Convert => GetMemberExpression(((UnaryExpression)expression).Operand),
            _ => throw new ArgumentException("Member expression expected"),
        };
    }

    /// <summary>
    /// Performs an INSERT INTO method which expression to copy values from another table
    /// </summary>
    /// <typeparam name="TFrom">The type of entity associated to the source table</typeparam>
    /// <typeparam name="TTo">The type of entity associated to the destination table</typeparam>
    /// <param name="expression">The expression of taking values from TFrom and assigning to TTo</param>
    public void Insert<TFrom, TTo>(Expression<Func<TFrom, TTo>> expression) {
        Insert<TTo, TFrom>(expression.Body);
    }

    /// <summary>
    /// Append OUTPUT to the insert statement to get the output identity of the inserted record.
    /// </summary>
    /// <typeparam name="TEntity">The type of the inserting entity</typeparam>
    public void OutputInsertColumn(string columnName) {
        SqliteColumn column = Table.Columns.First(column => column.Name == columnName);
        OutputInsertIdentity(column.Name);
    }

    private void Insert(Expression expression) {
        switch (expression.NodeType) {
            case ExpressionType.MemberInit:
                if (expression is not MemberInitExpression memberInitExpression)
                    throw new ArgumentException("Invalid expression");

                foreach (MemberBinding memberBinding in memberInitExpression.Bindings) {
                    if (memberBinding is MemberAssignment assignment) {
                        Insert(assignment);
                    }
                }

                break;

            case ExpressionType.NewArrayInit:
                if (expression is not NewArrayExpression newArrayExpression) {
                    throw new ArgumentException($"Invalid expression");
                }

                foreach (Expression recordInitExpression in newArrayExpression.Expressions) {
                    NextInsertRecord();
                    Insert(recordInitExpression);
                }

                break;

            default:
                throw new ArgumentException("Invalid expression");
        }
    }

    private void Insert(MemberAssignment assignmentExpression) {
        string columnName = GetColumnName(assignmentExpression);
        object? expressionValue = assignmentExpression.Expression.Execute();
        AssignInsertField(columnName, expressionValue);
    }

    private void Insert<TTo, TFrom>(Expression expression) {
        switch (expression.NodeType) {
            case ExpressionType.MemberInit:
                if (expression is not MemberInitExpression memberInitExpression) {
                    throw new ArgumentException("Invalid expression");
                }

                foreach (var memberBinding in memberInitExpression.Bindings) {
                    if (memberBinding is MemberAssignment assignment) {
                        Insert<TTo, TFrom>(assignment);
                    }
                }

                break;

            default:
                throw new ArgumentException("Invalid expression");
        }
    }

    private void Insert<TTo, TFrom>(MemberAssignment assignmentExpression) {
        var type = assignmentExpression.Expression.GetType();

        if (assignmentExpression.Expression is ConstantExpression constantExpression) {
            var columnName = GetColumnName(assignmentExpression);
            var expressionValue = constantExpression.Execute();
            AssignInsertField(columnName, expressionValue);

            return;
        }

        if (assignmentExpression.Expression is UnaryExpression unaryExpression) {
            var columnName = GetColumnName(assignmentExpression);
            var expressionValue = unaryExpression.Execute();
            AssignInsertField(columnName, expressionValue);

            return;
        }

        if (assignmentExpression.Expression is MemberExpression memberExpression) {
            var columnName = GetColumnName(assignmentExpression);
            var node = ResolveQuery(memberExpression);
            BuildInsertAssignmentSql(columnName, (dynamic)node);
            return;
        }

        else {

        }
    }

    private void BuildInsertAssignmentSql(string columnName, Node sourceNode) {
        if (sourceNode is MemberNode sourceMemberNode) {
            AssignInsertFieldFromSource(columnName, sourceMemberNode.TableName, sourceMemberNode.FieldName, GetSqlOperator(ExpressionType.Equal));
        }
        else {
            throw new ArgumentException($"{sourceNode.GetType()}");
        }
    }

    


    void ResolveNullValue(MemberNode memberNode, ExpressionType op) {
        switch (op) {
            case ExpressionType.Equal:
                QueryByFieldNull(memberNode.TableName, memberNode.FieldName);
                break;
            case ExpressionType.NotEqual:
                QueryByFieldNotNull(memberNode.TableName, memberNode.FieldName);
                break;
        }
    }

    void ResolveSingleOperation(ExpressionType op) {
        switch (op) {
            case ExpressionType.Not:
                AddNot();
                break;
        }
    }

    public void ResolveQuery<T>(Expression<Func<T, bool>> expression) {
        var expressionTree = ResolveQuery(expression.Body);
        BuildSql(expressionTree);
    }

    private Node ResolveQuery(Expression expression) {
        switch (expression) {
            case ConstantExpression constantExpression:
                return new ValueNode() {
                    Value = constantExpression.Value
                };
            case UnaryExpression unaryExpression:
                return new SingleOperationNode() {
                    Operator = unaryExpression.NodeType,
                    Child = ResolveQuery(unaryExpression.Operand),
                };
            case BinaryExpression binaryExpression:
                return new OperationNode() {
                    Left = ResolveQuery(binaryExpression.Left),
                    Operator = binaryExpression.NodeType,
                    Right = ResolveQuery(binaryExpression.Right),
                };
            case MethodCallExpression methodCallExpression:
                if (Enum.TryParse(methodCallExpression.Method.Name, true, out LikeMethod callFunction)) {
                    MemberExpression member = (MemberExpression)methodCallExpression.Object!;
                    string fieldValue = (string)methodCallExpression.Arguments.First().Execute()!;

                    return new LikeNode {
                        MemberNode = new MemberNode {
                            TableName = GetTableName(member),
                            FieldName = GetColumnName(methodCallExpression.Object)
                        },
                        Method = callFunction,
                        Value = fieldValue,
                    };
                }

                object? value = methodCallExpression.Execute();
                return new ValueNode() {
                    Value = value,
                };
            default:
                throw new NotSupportedException($"Expression type '{expression.Type}' is currently not supported");
        }
    }
    private Node ResolveQuery(MemberExpression memberExpression, MemberExpression? rootExpression) {
        rootExpression ??= memberExpression;

        return memberExpression.Expression.NodeType switch {
            ExpressionType.Parameter => new MemberNode {
                TableName = GetTableName(rootExpression),
                FieldName = GetColumnName(rootExpression)
            },
            ExpressionType.MemberAccess => ResolveQuery((MemberExpression)memberExpression.Expression, rootExpression),
            ExpressionType.Call or ExpressionType.Constant => new ValueNode() {
                Value = rootExpression.Execute()
            },
            _ => throw new ArgumentException("Expected member expression"),
        };
    }


    public void QueryByIsIn<T>(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        string columnName = GetColumnName(expression);
        QueryByIsIn(GetTableName<T>(), columnName, sqlQuery);
    }

    public void QueryByIsIn<T>(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        string columnName = GetColumnName(expression);
        QueryByIsIn(GetTableName<T>(), columnName, values);
    }

    public void QueryByNotIn<T>(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        string columnName = GetColumnName(expression);
        AddNot();
        QueryByIsIn(GetTableName<T>(), columnName, sqlQuery);
    }

    public void QueryByNotIn<T>(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        string columnName = GetColumnName(expression);
        AddNot();
        QueryByIsIn(GetTableName<T>(), columnName, values);
    }

    public void Join<T1, T2>(Expression<Func<T1, T2, bool>> expression) {
        BinaryExpression joinExpression = expression.Body as BinaryExpression
            ?? throw new ArgumentException("binary expression expected", nameof(expression));
        MemberExpression leftExpression = GetMemberExpression(joinExpression.Left);
        MemberExpression rightExpression = GetMemberExpression(joinExpression.Right);

        Join<T1, T2>(leftExpression, rightExpression);
    }

    public void Join<T1, T2, TKey>(Expression<Func<T1, TKey>> leftExpression, Expression<Func<T1, TKey>> rightExpression) {
        Join<T1, T2>(GetMemberExpression(leftExpression.Body), GetMemberExpression(rightExpression.Body));
    }

    public void Join<T1, T2>(MemberExpression leftExpression, MemberExpression rightExpression) {
        Join(GetTableName<T1>(), GetTableName<T2>(), GetColumnName(leftExpression), GetColumnName(rightExpression));
    }

    public void OrderBy<T>(Expression<Func<T, object>> expression, bool descending = false) {
        string columnName = GetColumnName(GetMemberExpression(expression.Body));
        OrderBy(GetTableName<T>(), columnName, descending);
    }

    public void Select<T>(Expression<Func<T, object>> expression) {
        Select<T>(expression.Body);
    }

    private void Select<T>(Expression expression) {
        switch (expression.NodeType) {
            case ExpressionType.Parameter:
                Select(GetTableName(expression.Type));
                break;
            case ExpressionType.Convert:
            case ExpressionType.MemberAccess:
                Select<T>(GetMemberExpression(expression));
                break;
            case ExpressionType.New:
                foreach (MemberExpression memberExp in (expression as NewExpression).Arguments)
                    Select<T>(memberExp);
                break;
            case ExpressionType.MemberInit:
                if (expression is MemberInitExpression memberInitExpression) {
                    foreach (var memberExp in memberInitExpression.Bindings) {
                        if (memberExp is MemberAssignment assignmentExpression) {
                            Select<T>(assignmentExpression.Expression);
                        }
                    }
                    break;
                }

                throw new ArgumentException("Invalid expression");
            default:
                throw new ArgumentException("Invalid expression");
        }
    }

    private void Select<T>(MemberExpression expression) {
        if (expression.Type.IsClass && expression.Type != typeof(String))
            Select(GetTableName(expression.Type));
        else
            Select(GetTableName<T>(), GetColumnName(expression));
    }

    public void SelectWithFunction<T>(Expression<Func<T, object>> expression, SelectFunction selectFunction) {
        SelectWithFunction<T>(expression.Body, selectFunction);
    }

    private void SelectWithFunction<T>(Expression expression, SelectFunction selectFunction) {
        string columnName = GetColumnName(GetMemberExpression(expression));
        Select(GetTableName<T>(), columnName, selectFunction);
    }

    public void SelectWithFunction<T>(SelectFunction selectFunction) {
        Select(selectFunction);
    }

    public void GroupBy<T>(Expression<Func<T, object>> expression) {
        GroupBy<T>(GetMemberExpression(expression.Body));
    }

    private void GroupBy<T>(MemberExpression expression) {
        string columnName = GetColumnName(GetMemberExpression(expression));
        GroupBy(GetTableName<T>(), columnName);
    }

    public void Update<T>(Expression<Func<T, object>> expression) {
        Update<T>(expression.Body);
    }

    private void Update<T>(Expression expression) {
        switch (expression.NodeType) {
            case ExpressionType.New:
                foreach (MemberExpression memberExp in (expression as NewExpression).Arguments)
                    Update<T>(memberExp);
                break;
            case ExpressionType.MemberInit:
                if (!(expression is MemberInitExpression memberInitExpression))
                    throw new ArgumentException("Invalid expression");

                foreach (var memberBinding in memberInitExpression.Bindings) {
                    if (memberBinding is MemberAssignment assignment) {
                        Update<T>(assignment);
                    }
                }

                break;
            default:
                throw new ArgumentException("Invalid expression");
        }
    }

    private void Update<T>(MemberExpression expression) {
        throw new NotImplementedException();
    }

    private void Update<T>(MemberAssignment assignmentExpression) {
        if (assignmentExpression.Expression is BinaryExpression expression) {
            UpdateFieldWithOperation(
                GetColumnName(expression.Left),
                expression.Right.Execute(),
                GetSqlOperator(assignmentExpression.Expression.NodeType)
            );
        }
        else if (assignmentExpression.Expression is UnaryExpression unaryExpression) {
            var columnName = GetColumnName(assignmentExpression);
            var expressionValue = unaryExpression.Execute();
            UpdateAssignField(columnName, expressionValue);
        }
        else if (assignmentExpression.Expression is MethodCallExpression mce) {
            ResolveUpdateMethodCall(mce);
        }
    }

    private void ResolveUpdateMethodCall(MethodCallExpression callExpression) {
        object?[] arguments = callExpression.Arguments.Select(argument => argument.Execute()).ToArray();
        var resolver = UpdateResolvers.FirstOrDefault(_ => _.SupportedMethod == callExpression.Method) ??
            throw new NotSupportedException($"The provided method expression {callExpression.Method.DeclaringType.Name}.{callExpression.Method.Name}() is not supported");
        ResolveStatement(Builder, callExpression, arguments);
    }

    

    public void InsertTo<TTo>() {
        TableNames.Add(GetTableName<TTo>());
    }

    /// <summary>
    /// Updates specified <see cref="columnName"/> with assigning <see cref="value"/>
    /// </summary>
    /// <param name="columnName"></param>
    /// <param name="value"></param>
    public void AssignInsertField(string columnName, object? value) {
        if (Operation is not (SqlOperation.Insert or SqlOperation.InsertFrom)) {
            throw new InvalidOperationException($"Statement must be INSERT to assign field");
        }

        string parameterName = AddParameter(value);
        string? updateValue = parameterName;

        var lastInsertRecord = InsertValues.LastOrDefault() ?? NextInsertRecord();
        lastInsertRecord.Add(columnName.SqlQuote(), updateValue);
    }

    public void AssignInsertFieldFromSource(string columnName, string sourceTableName, string sourceFieldName, string op) {
        Dictionary<string, object?> lastInsertRecord = InsertValues.LastOrDefault() ?? NextInsertRecord();

        string updateValue = FormatSqlMember(sourceTableName, sourceFieldName);

        lastInsertRecord.Add(columnName.SqlQuote(), updateValue);
    }

    public Dictionary<string, object?> NextInsertRecord() {
        Dictionary<string, object?> nextInsertRecord = [];
        InsertValues.Add(nextInsertRecord);
        return nextInsertRecord;
    }

    public void OutputInsertIdentity(string columnName) {
        if (Operation is not SqlOperation.Insert) {
            throw new InvalidOperationException($"Statement must be INSERT to OUTPUT column");
        }
        _insertOutput = columnName.SqlQuote();
    }

    

    private string GenerateQueryCommand() {
        if (!_pageSize.HasValue || _pageSize == 0) {
            return Adapter.QueryString(Selection, Source, Conditions, Grouping, Having, Order);
        }
        if (_pageIndex > 0 && OrderByList.Count == 0) {
            throw new Exception("Pagination requires the ORDER BY statement to be specified");
        }
        return Adapter.QueryStringPage(Source, Selection, Conditions, Order, _pageSize.Value, _pageIndex);
    }

    /// <summary>
    /// Updates specified <see cref="columnName"/> with assigning <see cref="value"/>
    /// </summary>
    /// <param name="columnName"></param>
    /// <param name="value"></param>
    public void UpdateAssignField(string columnName, object value) {
        var paramId = GenerateParameterName();
        CommandParameters.Add(paramId, value);
        var updateValue = $"{Adapter.Field(columnName)} = {Adapter.Parameter(paramId)}";
        UpdateValues.Add(updateValue);
    }

    /// <summary>
    /// Updates specified <paramref name="columnName"/> by replacing <paramref name="oldValue"/> with <paramref name="newValue"/>.
    /// </summary>
    public void UpdateColumnReplaceString(string columnName, object? oldValue, object? newValue) {
        string updateSql = $"{columnName.SqlQuote()} = REPLACE({columnName.SqlQuote()}, {AddParameter(oldValue)}, {AddParameter(newValue)})";
        UpdateValues.Add(updateSql);
    }

    /// <summary>
    /// Updates specified <see cref="columnName"/> by performing the <see cref="operation"/> of the <see cref="operandValue"/> to current value
    /// </summary>
    /// <param name="columnName">The name of field to update</param>
    /// <param name="operandValue">The other operand of the operation</param>
    /// <param name="operation">The operation</param>
    public void UpdateFieldWithOperation(string columnName, object? operandValue, string operation) {
        string paramId = AddParameter(operandValue);
        var updateValue = $"{Adapter.Field(columnName)} = {Adapter.Field(columnName)} {operation} {Adapter.Parameter(paramId)}";
        UpdateValues.Add(updateValue);
    }
}