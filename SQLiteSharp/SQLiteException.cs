namespace SQLiteSharp;

public class SqliteException(Result result, string message) : Exception(message) {
    public Result Result { get; } = result;
}
public class NotNullConstraintViolationException : SqliteException {
    public IEnumerable<ColumnMap>? Columns { get; }

    public NotNullConstraintViolationException(Result result, string message, TableMap? mapping, object? obj)
        : base(result, message) {
        if (mapping is not null && obj is not null) {
            Columns = mapping.Columns.Where(column => column.NotNull && column.GetValue(obj) is null);
        }
    }
    public NotNullConstraintViolationException(Result result, string message)
        : this(result, message, null, null) {
    }
    public NotNullConstraintViolationException(SqliteException exception, TableMap mapping, object obj)
        : this(exception.Result, exception.Message, mapping, obj) {
    }
}