using System.Linq.Expressions;

namespace SQLiteSharp;

/// <summary>
/// Query to an asynchronous database connection.
/// </summary>
public class AsyncTableQuery<T>(TableQuery<T> query) where T : new() {
    private readonly TableQuery<T> _query = query;

    private Task<T2> LockAsync<T2>(Func<SQLiteConnection, T2> function) {
        return Task.Run(() => function(_query.Connection));
    }

    /// <summary>
    /// Filters the query based on a predicate.
    /// </summary>
    public AsyncTableQuery<T> Where(Expression<Func<T, bool>> predExpr) {
        return new AsyncTableQuery<T>(_query.Where(predExpr));
    }

    /// <summary>
    /// Skips a given number of elements from the query and then yields the remainder.
    /// </summary>
    public AsyncTableQuery<T> Skip(int n) {
        return new AsyncTableQuery<T>(_query.Skip(n));
    }

    /// <summary>
    /// Yields a given number of elements from the query and then skips the remainder.
    /// </summary>
    public AsyncTableQuery<T> Take(int n) {
        return new AsyncTableQuery<T>(_query.Take(n));
    }

    /// <summary>
    /// Order the query results according to a key.
    /// </summary>
    public AsyncTableQuery<T> OrderBy<U>(Expression<Func<T, U>> orderExpr) {
        return new AsyncTableQuery<T>(_query.OrderBy(orderExpr));
    }

    /// <summary>
    /// Order the query results according to a key.
    /// </summary>
    public AsyncTableQuery<T> OrderByDescending<U>(Expression<Func<T, U>> orderExpr) {
        return new AsyncTableQuery<T>(_query.OrderByDescending(orderExpr));
    }

    /// <summary>
    /// Queries the database and returns the results as a List.
    /// </summary>
    public Task<List<T>> ToListAsync() {
        return LockAsync(connection => _query.ToList());
    }

    /// <summary>
    /// Queries the database and returns the results as an array.
    /// </summary>
    public Task<T[]> ToArrayAsync() {
        return LockAsync(connection => _query.ToArray());
    }

    /// <summary>
    /// Execute SELECT COUNT(*) on the query
    /// </summary>
    public Task<int> CountAsync() {
        return LockAsync(connection => _query.Count());
    }

    /// <summary>
    /// Execute SELECT COUNT(*) on the query with an additional WHERE clause.
    /// </summary>
    public Task<int> CountAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.Count(predExpr));
    }

    /// <summary>
    /// Returns the element at a given index
    /// </summary>
    public Task<T> ElementAtAsync(int index) {
        return LockAsync(connection => _query.ElementAt(index));
    }

    /// <summary>
    /// Returns the first element of this query.
    /// </summary>
    public Task<T> FirstAsync() {
        return LockAsync(connection => _query.First());
    }

    /// <summary>
    /// Returns the first element of this query, or null if no element is found.
    /// </summary>
    public Task<T?> FirstOrDefaultAsync() {
        return LockAsync(connection => _query.FirstOrDefault());
    }

    /// <summary>
    /// Returns the first element of this query that matches the predicate.
    /// </summary>
    public Task<T> FirstAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.First(predExpr));
    }

    /// <summary>
    /// Returns the first element of this query that matches the predicate.
    /// </summary>
    public Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.FirstOrDefault(predExpr));
    }

    /// <summary>
    /// Delete all the rows that match this query and the given predicate.
    /// </summary>
    public Task<int> DeleteAsync(Expression<Func<T, bool>> predExpr) {
        return LockAsync(connection => _query.Delete(predExpr));
    }

    /// <summary>
    /// Delete all the rows that match this query.
    /// </summary>
    public Task<int> DeleteAsync() {
        return LockAsync(connection => _query.Delete());
    }
}