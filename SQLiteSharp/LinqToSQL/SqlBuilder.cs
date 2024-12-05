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
    public int CurrentParameterIndex { get; private set; }

    private SqlOperation Operation = SqlOperation.Query;
    private List<string> SplitColumns = [];

    private readonly List<string> _updateValues = [];
    private readonly List<string> TableNames = [];
    private readonly List<string> JoinExpressions = [];
    private readonly List<string> SelectionList = [];
    private readonly List<string> WhereConditions = [];
    private readonly List<string> OrderByList = [];
    private readonly List<string> GroupByList = [];
    private readonly List<string> HavingConditions = [];

    private int? _pageSize;
    private int _pageIndex;

    private string _insertOutput = "";

    public SqlBuilder<T> Where(Expression<Func<T, bool>> expression) {
        And();
        ResolveQuery(expression);
        return this;
    }

    public SqlBuilder<T> WhereIsIn(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        And();
        QueryByIsIn(expression, sqlQuery);
        return this;
    }

    public SqlBuilder<T> WhereIsIn(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        And();
        QueryByIsIn(expression, values);
        return this;
    }

    public SqlBuilder<T> WhereNotIn(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        And();
        QueryByNotIn(expression, sqlQuery);
        return this;
    }

    public SqlBuilder<T> WhereNotIn(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        And();
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

    public SqlBuilder<T> Select(params Expression<Func<T, object>>[] expressions) {
        foreach (var expression in expressions)
            Select(expression);

        return this;
    }

    public SqlBuilder<T> Select<TResult>(Expression<Func<T, TResult>> expression) {
        Select(expression.Body);
        return this;
    }

    public SqlBuilder<T> Update(Expression<Func<T, object>> expression) {
        Update(expression);
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
        Insert(expression);
        return this;
    }

    /// <summary>
    /// Performs insert to <see cref="TTo"/> table using the values copied from the given expression
    /// </summary>
    /// <typeparam name="TTo">The destination table</typeparam>
    /// <param name="expression">The expression describes how to copy values from original table <see cref="T"/></param>
    /// <returns></returns>
    public SqlBuilder<T> Insert<TTo>(Expression<Func<T, TTo>> expression) {
        InsertTo<TTo>();
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

    private static readonly List<UpdateMethodResolver> StatementResolvers = [new StringReplaceResolver()];

    public static string GetSqlOperator(ExpressionType expressionType) => expressionType switch {
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
        _ => throw new NotSupportedException($"Cannot get SQL operator for {expressionType}")
    };

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
    /// Prepares an INSERT INTO method which expression to copy values from another table
    /// </summary>
    /// <typeparam name="T">The type of entity that associates to the table to insert record(s) to</typeparam>
    /// <param name="expression">The expression that generates the record(s) to insert</param>
    public void Insert(Expression<Func<T, IEnumerable<T>>> expression) {
        Insert(expression.Body);
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

    void BuildInsertAssignmentSql(string columnName, MemberNode sourceNode) {
        AssignInsertFieldFromSource(columnName, sourceNode.TableName, sourceNode.FieldName, GetSqlOperator(ExpressionType.Equal));
    }

    void BuildInsertAssignmentSql(string columnName, Node sourceNode) {
        throw new ArgumentException($"Unsupported resolution of Node type");
    }

    void BuildSql(Node node) {
        BuildSql((dynamic)node);
    }

    void BuildSql(LikeNode node) {
        if (node.Method is LikeMethod.Equals) {
            QueryByField(node.MemberNode.TableName, node.MemberNode.FieldName, GetSqlOperator(ExpressionType.Equal), node.Value);
        }
        else {
            string? value = node.Method switch {
                LikeMethod.Equals => node.Value,
                LikeMethod.StartsWith => node.Value + "%",
                LikeMethod.EndsWith => "%" + node.Value,
                LikeMethod.Contains => "%" + node.Value + "%",
                _ => throw new NotImplementedException($"'{node.Method}'")
            };
            QueryByFieldLike(node.MemberNode.TableName, node.MemberNode.FieldName, value);
        }
    }

    void BuildSql(OperationNode node) {
        BuildSql((dynamic)node.Left, (dynamic)node.Right, node.Operator);
    }

    void BuildSql(MemberNode memberNode) {
        QueryByField(memberNode.TableName, memberNode.FieldName, GetSqlOperator(ExpressionType.Equal), true);
    }

    void BuildSql(SingleOperationNode node) {
        if (node.Operator is ExpressionType.Not) {
            Not();
        }
        BuildSql(node.Child);
    }

    void BuildSql(MemberNode memberNode, ValueNode valueNode, ExpressionType op) {
        if (valueNode.Value is null) {
            ResolveNullValue(memberNode, op);
        }
        else {
            QueryByField(memberNode.TableName, memberNode.FieldName, GetSqlOperator(op), valueNode.Value);
        }
    }

    void BuildSql(ValueNode valueNode, MemberNode memberNode, ExpressionType op) {
        BuildSql(memberNode, valueNode, op);
    }

    void BuildSql(MemberNode leftMember, MemberNode rightMember, ExpressionType op) {
        QueryByFieldComparison(leftMember.TableName, leftMember.FieldName, GetSqlOperator(op), rightMember.TableName, rightMember.FieldName);
    }

    void BuildSql(SingleOperationNode leftMember, Node rightMember, ExpressionType op) {
        if (leftMember.Operator == ExpressionType.Not)
            BuildSql(leftMember as Node, rightMember, op);
        else
            BuildSql((dynamic)leftMember.Child, (dynamic)rightMember, op);
    }

    void BuildSql(Node leftMember, SingleOperationNode rightMember, ExpressionType op) {
        BuildSql(rightMember, leftMember, op);
    }

    void BuildSql(Node leftNode, Node rightNode, ExpressionType op) {
        BeginExpression();
        BuildSql((dynamic)leftNode);
        ResolveOperation(op);
        BuildSql((dynamic)rightNode);
        EndExpression();
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
                Not();
                break;
        }
    }

    void ResolveOperation(ExpressionType op) {
        switch (op) {
            case ExpressionType.And:
            case ExpressionType.AndAlso:
                And();
                break;
            case ExpressionType.Or:
            case ExpressionType.OrElse:
                Or();
                break;
            default:
                throw new ArgumentException(string.Format("Unrecognized binary expression operation '{0}'", op.ToString()));
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
        var fieldName = GetColumnName(expression);
        QueryByIsIn(GetTableName<T>(), fieldName, sqlQuery);
    }

    public void QueryByIsIn<T>(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        var fieldName = GetColumnName(expression);
        QueryByIsIn(GetTableName<T>(), fieldName, values);
    }

    public void QueryByNotIn<T>(Expression<Func<T, object>> expression, SqlBuilder sqlQuery) {
        var fieldName = GetColumnName(expression);
        Not();
        QueryByIsIn(GetTableName<T>(), fieldName, sqlQuery);
    }

    public void QueryByNotIn<T>(Expression<Func<T, object>> expression, IEnumerable<object> values) {
        var fieldName = GetColumnName(expression);
        Not();
        QueryByIsIn(GetTableName<T>(), fieldName, values);
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

    public void OrderBy<T>(Expression<Func<T, object>> expression, bool desc = false) {
        var fieldName = GetColumnName(GetMemberExpression(expression.Body));
        OrderBy(GetTableName<T>(), fieldName, desc);
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
        var fieldName = GetColumnName(GetMemberExpression(expression));
        Select(GetTableName<T>(), fieldName, selectFunction);
    }

    public void SelectWithFunction<T>(SelectFunction selectFunction) {
        Select(selectFunction);
    }

    public void GroupBy<T>(Expression<Func<T, object>> expression) {
        GroupBy<T>(GetMemberExpression(expression.Body));
    }

    private void GroupBy<T>(MemberExpression expression) {
        var fieldName = GetColumnName(GetMemberExpression(expression));
        GroupBy(GetTableName<T>(), fieldName);
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
        var resolver = StatementResolvers.FirstOrDefault(_ => _.SupportedMethod == callExpression.Method) ??
            throw new NotSupportedException($"The provided method expression {callExpression.Method.DeclaringType.Name}.{callExpression.Method.Name}() is not supported");
        ResolveStatement(Builder, callExpression, arguments);
    }

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
    private string UpdateValues => string.Join(", ", _updateValues);
    private List<Dictionary<string, object?>> InsertValues { get; } = [];
    private string Order => OrderByList.Count == 0 ? "" : "ORDER BY " + string.Join(", ", OrderByList);
    private string Grouping => GroupByList.Count == 0 ? "" : "GROUP BY " + string.Join(", ", GroupByList);
    private string Having => HavingConditions.Count == 0 ? "" : "HAVING " + string.Join(" ", HavingConditions);

    public void InsertTo<TTo>() {
        TableNames.Add(GetTableName<TTo>());
    }

    /// <summary>
    /// Updates specified <see cref="fieldName"/> with assigning <see cref="value"/>
    /// </summary>
    /// <param name="fieldName"></param>
    /// <param name="value"></param>
    public void AssignInsertField(string fieldName, object? value) {
        if (Operation is not (SqlOperation.Insert or SqlOperation.InsertFrom)) {
            throw new InvalidOperationException($"Statement must be INSERT to assign field");
        }

        string paramId = AddParameter(value);
        string? updateValue = $"@{paramId}";

        var lastInsertRecord = InsertValues.LastOrDefault() ?? NextInsertRecord();
        lastInsertRecord.Add(fieldName.SqlQuote(), updateValue);
    }

    public void AssignInsertFieldFromSource(string fieldName, string sourceTableName, string sourceFieldName, string op) {
        Dictionary<string, object?> lastInsertRecord = InsertValues.LastOrDefault() ?? NextInsertRecord();

        string updateValue = $"{sourceTableName.SqlQuote()}.{sourceFieldName.SqlQuote()}";

        lastInsertRecord.Add(fieldName.SqlQuote(), updateValue);
    }

    public Dictionary<string, object?> NextInsertRecord() {
        Dictionary<string, object?> nextInsertRecord = [];
        InsertValues.Add(nextInsertRecord);
        return nextInsertRecord;
    }

    public void OutputInsertIdentity(string fieldName) {
        if (Operation is not SqlOperation.Insert) {
            throw new InvalidOperationException($"Statement must be INSERT to OUTPUT column");
        }
        _insertOutput = fieldName.SqlQuote();
    }

    public void Join(string originalTableName, string joinTableName, string leftField, string rightField) {
        var joinString =
            $"JOIN {Adapter.Table(joinTableName)} " +
            $"ON {Adapter.Field(originalTableName, leftField)} = {Adapter.Field(joinTableName, rightField)}";

        TableNames.Add(joinTableName);
        JoinExpressions.Add(joinString);
        SplitColumns.Add(rightField);
    }

    public void OrderBy(string tableName, string fieldName, bool desc = false) {
        var order = Adapter.Field(tableName, fieldName);
        if (desc)
            order += " DESC";

        OrderByList.Add(order);
    }

    public void Select(string tableName) {
        var selectionString = $"{Adapter.Table(tableName)}.*";
        SelectionList.Add(selectionString);
    }

    public void Select(string tableName, string fieldName) {
        SelectionList.Add(Adapter.Field(tableName, fieldName));
    }

    public void Select(string tableName, string fieldName, SelectFunction selectFunction) {
        var selectionString = $"{selectFunction}({Adapter.Field(tableName, fieldName)})";
        SelectionList.Add(selectionString);
    }

    public void Select(SelectFunction selectFunction) {
        var selectionString = $"{selectFunction}(*)";
        SelectionList.Add(selectionString);
    }

    public void GroupBy(string tableName, string fieldName) {
        GroupByList.Add(Adapter.Field(tableName, fieldName));
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
    /// Updates specified <see cref="fieldName"/> with assigning <see cref="value"/>
    /// </summary>
    /// <param name="fieldName"></param>
    /// <param name="value"></param>
    public void UpdateAssignField(string fieldName, object value) {
        var paramId = GenerateParameterName();
        CommandParameters.Add(paramId, value);
        var updateValue = $"{Adapter.Field(fieldName)} = {Adapter.Parameter(paramId)}";
        _updateValues.Add(updateValue);
    }

    /// <summary>
    /// Updates specified <paramref name="columnName"/> by replacing <paramref name="oldValue"/> with <paramref name="newValue"/>.
    /// </summary>
    public void UpdateColumnReplaceString(string columnName, object? oldValue, object? newValue) {
        string updateSql = $"{columnName.SqlQuote()} = REPLACE({columnName.SqlQuote()}, @{AddParameter(oldValue)}, @{AddParameter(newValue)})";
        _updateValues.Add(updateSql);
    }

    /// <summary>
    /// Updates specified <see cref="fieldName"/> by performing the <see cref="operation"/> of the <see cref="operandValue"/> to current value
    /// </summary>
    /// <param name="fieldName">The name of field to update</param>
    /// <param name="operandValue">The other operand of the operation</param>
    /// <param name="operation">The operation</param>
    public void UpdateFieldWithOperation(string fieldName, object? operandValue, string operation) {
        string paramId = AddParameter(operandValue);
        var updateValue = $"{Adapter.Field(fieldName)} = {Adapter.Field(fieldName)} {operation} {Adapter.Parameter(paramId)}";
        _updateValues.Add(updateValue);
    }

    public void BeginExpression() {
        WhereConditions.Add("(");
    }

    public void EndExpression() {
        WhereConditions.Add(")");
    }

    private void And() {
        if (WhereConditions.Count > 0) {
            WhereConditions.Add(" AND ");
        }
    }
    private void Or() {
        if (WhereConditions.Count > 0) {
            WhereConditions.Add(" OR ");
        }
    }
    private void Not() {
        WhereConditions.Add(" NOT ");
    }

    public void QueryByField(string tableName, string fieldName, string op, object fieldValue) {
        var paramId = GenerateParameterName();
        var newCondition = string.Format("{0} {1} {2}",
                                         Adapter.Field(tableName, fieldName),
                                         op,
                                         Adapter.Parameter(paramId));

        WhereConditions.Add(newCondition);
        CommandParameters.Add(paramId, fieldValue);
    }

    public void QueryByFieldLike(string tableName, string fieldName, string fieldValue) {
        var paramId = GenerateParameterName();
        var newCondition = string.Format("{0} LIKE {1}",
                                         Adapter.Field(tableName, fieldName),
                                         Adapter.Parameter(paramId));

        WhereConditions.Add(newCondition);
        CommandParameters.Add(paramId, fieldValue);
    }

    public void QueryByFieldNull(string tableName, string fieldName) {
        WhereConditions.Add(string.Format("{0} IS NULL", Adapter.Field(tableName, fieldName)));
    }

    public void QueryByFieldNotNull(string tableName, string fieldName) {
        WhereConditions.Add(string.Format("{0} IS NOT NULL", Adapter.Field(tableName, fieldName)));
    }

    public void QueryByFieldComparison(string leftTableName, string leftFieldName, string op,
                                       string rightTableName, string rightFieldName) {
        var newCondition = string.Format("{0} {1} {2}",
                                         Adapter.Field(leftTableName, leftFieldName),
                                         op,
                                         Adapter.Field(rightTableName, rightFieldName));

        WhereConditions.Add(newCondition);
    }

    public void QueryByIsIn(string tableName, string fieldName, SqlBuilder sqlQuery) {
        var innerQuery = sqlQuery.CommandText;
        foreach (var param in sqlQuery.CommandParameters) {
            var innerParamKey = "Inner" + param.Key;
            innerQuery = Regex.Replace(innerQuery, param.Key, innerParamKey);
            CommandParameters.Add(innerParamKey, param.Value);
        }

        var newCondition = string.Format("{0} IN ({1})", Adapter.Field(tableName, fieldName), innerQuery);

        WhereConditions.Add(newCondition);
    }

    public void QueryByIsIn(string tableName, string fieldName, IEnumerable<object> values) {
        var paramIds = values.Select(x => {
            string parameterId = GenerateParameterName();
            CommandParameters.Add(parameterId, x);
            return Adapter.Parameter(parameterId);
        });

        var newCondition = string.Format("{0} IN ({1})", Adapter.Field(tableName, fieldName), string.Join(",", paramIds));
        WhereConditions.Add(newCondition);
    }

    private string GenerateParameterName() {
        CurrentParameterIndex++;
        return $"Parameter{CurrentParameterIndex}";
    }
    private string AddParameter(object? value) {
        string name = GenerateParameterName();
        CommandParameters.Add(name, value);
        return name;
    }
}