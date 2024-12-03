using System.Collections;
using System.Linq.Expressions;

namespace SQLiteSharp;

public class TableQuery<T>(SQLiteConnection connection, TableMap table) : IEnumerable<T>, IEnumerable, IAsyncEnumerable<T> {
    public SQLiteConnection Connection { get; } = connection;
    public TableMap Table { get; } = table;

    private Expression? WhereExpression;
    private List<(string ColumnName, bool Ascending)>? OrderBys;
    private int? Limit;
    private int? Offset;

    public TableQuery(SQLiteConnection connection)
        : this(connection, connection.MapTable<T>()) {
    }

    public TableQuery<U> Clone<U>() {
        TableQuery<U> query = new(Connection, Table) {
            WhereExpression = WhereExpression,
            OrderBys = OrderBys?.ToList(),
            Limit = Limit,
            Offset = Offset,
        };
        return query;
    }

    /// <summary>
    /// Filters the query based on a predicate.
    /// </summary>
    public TableQuery<T> Where(Expression<Func<T, bool>> predicate) {
        TableQuery<T> query = Clone<T>();
        query.WhereExpression = WhereExpression.AndAlso(predicate.Body);
        return query;
    }

    /// <summary>
    /// Delete all the rows that match this query (and the given predicate).
    /// </summary>
    public int Delete(Expression<Func<T, bool>>? predicate = null) {
        if (Limit is not null || Offset is not null) {
            throw new InvalidOperationException("Cannot delete with limits or offsets");
        }

        Expression? deletePredicate = WhereExpression.AndAlso(predicate)
            ?? throw new InvalidOperationException($"No delete condition (use SQLiteConnection.DeleteAll to delete every item from the table)");
        
        List<object?> parameters = [];
        string commandText = $"delete from {Table.TableName.SqlQuote()} where {ExpressionToSql(deletePredicate, parameters).CommandText}";
        SQLiteCommand command = Connection.CreateCommand(commandText, parameters);

        int rowCount = command.ExecuteNonQuery();
        return rowCount;
    }

    /// <summary>
    /// Yields a given number of elements from the query and then skips the remainder.
    /// </summary>
    public TableQuery<T> Take(int n) {
        TableQuery<T> query = Clone<T>();
        query.Limit = n;
        return query;
    }
    /// <summary>
    /// Skips a given number of elements from the query and then yields the remainder.
    /// </summary>
    public TableQuery<T> Skip(int n) {
        TableQuery<T> query = Clone<T>();
        query.Offset = n;
        return query;
    }

    /// <summary>
    /// Returns the element at a given index.
    /// </summary>
    public T ElementAt(int index) {
        return Skip(index).Take(1).First();
    }

    /// <summary>
    /// Orders the query results by a key ascending.
    /// </summary>
    public TableQuery<T> OrderBy<U>(Expression<Func<T, U>> expression) {
        return AddOrderBy(expression, true);
    }
    /// <summary>
    /// Orders the query results by a key descending.
    /// </summary>
    public TableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> expression) {
        return AddOrderBy(expression, false);
    }

    private TableQuery<T> AddOrderBy<U>(Expression<Func<T, U>> orderExpression, bool ascending) {
        MemberExpression? memberExpression = orderExpression.Body as MemberExpression;
        // Unwrap type cast
        if (orderExpression.Body is UnaryExpression body && body.NodeType is ExpressionType.Convert) {
            memberExpression = body.Operand as MemberExpression;
        }

        if (memberExpression?.Expression?.NodeType is ExpressionType.Parameter) {
            TableQuery<T> query = Clone<T>();
            query.OrderBys ??= [];
            query.OrderBys.Add((Table.FindColumnByMemberName(memberExpression.Member.Name)!.Name, ascending));
            return query;
        }
        else {
            throw new NotSupportedException($"Order By does not support: '{orderExpression}'");
        }
    }

    private SqlExpression ExpressionToSql(Expression expression, IList<object?> parameters) {
        return SqlExpression.FromExpression(
            expression,
            parameters,
            MemberName => Table.FindColumnByMemberName(MemberName)!.Name
        );
    }
    private SQLiteCommand GenerateCommand(string selectionList) {
        string commandText = $"select {selectionList} from {Table.TableName.SqlQuote()}";
        List<object?> parameters = [];
        if (WhereExpression is not null) {
            commandText += $" where {ExpressionToSql(WhereExpression, parameters).CommandText}";
        }
        if (OrderBys?.Count > 0) {
            string orderByString = string.Join(", ", OrderBys.Select(orderBy => orderBy.ColumnName.SqlQuote() + (orderBy.Ascending ? "" : " desc")));
            commandText += $" order by {orderByString}";
        }
        if (Limit is not null) {
            commandText += $" limit {Limit.Value}";
        }
        if (Offset is not null) {
            if (Limit is null) {
                commandText += " limit -1 ";
            }
            commandText += $" offset {Offset.Value}";
        }
        return Connection.CreateCommand(commandText, parameters);
    }

    /// <summary>
    /// Returns the number of elements matching the query.
    /// </summary>
    public int Count() {
        return GenerateCommand("count(*)").ExecuteScalar<int>();
    }
    /// <inheritdoc cref="Count()"/>
    public long LongCount() {
        return GenerateCommand("count(*)").ExecuteScalar<long>();
    }
    /// <summary>
    /// Returns the number of elements matching the query and the predicate.
    /// </summary>
    public int Count(Expression<Func<T, bool>> predicate) {
        return Where(predicate).Count();
    }
    /// <inheritdoc cref="Count(Expression{Func{T, bool}})"/>
    public long LongCount(Expression<Func<T, bool>> predicate) {
        return Where(predicate).LongCount();
    }

    /// <summary>
    /// Returns an enumerator that iterates through the elements matching the query.
    /// </summary>
    public IEnumerator<T> GetEnumerator() {
        return GenerateCommand("*").ExecuteQuery<T>().GetEnumerator();
    }
    /// <inheritdoc cref="GetEnumerator()"/>
    IEnumerator IEnumerable.GetEnumerator() {
        return GetEnumerator();
    }
    /// <inheritdoc cref="GetEnumerator()"/>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancelToken = default) {
        return this.ToAsyncEnumerable().GetAsyncEnumerator(cancelToken);
    }

    /// <summary>
    /// Returns a list of all the elements matching the query.
    /// </summary>
    public List<T> ToList() {
        return GenerateCommand("*").ExecuteQuery<T>().ToList();
    }
    /// <summary>
    /// Returns an array of all the elements matching the query.
    /// </summary>
    public T[] ToArray() {
        return GenerateCommand("*").ExecuteQuery<T>().ToArray();
    }
    /// <summary>
    /// Returns the first element matching the query, or throws.
    /// </summary>
    public T First() {
        return GenerateCommand("*").ExecuteQuery<T>().First();
    }
    /// <summary>
    /// Returns the first element matching the query and the predicate, or throws.
    /// </summary>
    public T First(Expression<Func<T, bool>> predicate) {
        return Where(predicate).First();
    }
    /// <summary>
    /// Returns the first element matching the query, or <see langword="null"/> if no element is found.
    /// </summary>
    public T? FirstOrDefault() {
        return GenerateCommand("*").ExecuteQuery<T>().FirstOrDefault();
    }
    /// <summary>
    /// Returns the first element matching the query and the predicate, or <see langword="null"/> if no element is found.
    /// </summary>
    public T? FirstOrDefault(Expression<Func<T, bool>> predicate) {
        return Where(predicate).FirstOrDefault();
    }

    /// <inheritdoc cref="ToList()"/>
    public Task<List<T>> ToListAsync() {
        return Task.Run(ToList);
    }
    /// <inheritdoc cref="ToArray()"/>
    public Task<T[]> ToArrayAsync() {
        return Task.Run(ToArray);
    }
    /// <inheritdoc cref="Count()"/>
    public Task<int> CountAsync() {
        return Task.Run(Count);
    }
    /// <inheritdoc cref="Count(Expression{Func{T, bool}})"/>
    public Task<int> CountAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => Count(predicate));
    }
    /// <inheritdoc cref="ElementAt(int)"/>
    public Task<T> ElementAtAsync(int index) {
        return Task.Run(() => ElementAt(index));
    }
    /// <inheritdoc cref="First()"/>
    public Task<T> FirstAsync() {
        return Task.Run(First);
    }
    /// <inheritdoc cref="FirstOrDefault()"/>
    public Task<T?> FirstOrDefaultAsync() {
        return Task.Run(FirstOrDefault);
    }
    /// <inheritdoc cref="First(Expression{Func{T, bool}})"/>
    public Task<T> FirstAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => First(predicate));
    }
    /// <inheritdoc cref="FirstOrDefault(Expression{Func{T, bool}})"/>
    public Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => FirstOrDefault(predicate));
    }
    /// <inheritdoc cref="Delete(Expression{Func{T, bool}}?)"/>
    public Task<int> DeleteAsync(Expression<Func<T, bool>> predicate) {
        return Task.Run(() => Delete(predicate));
    }
}