namespace SQLiteSharp;

public class SQLiteException(SQLiteRaw.Result result, string message) : Exception(message) {
    public SQLiteRaw.Result Result { get; } = result;
}
public class NotNullConstraintViolationException : SQLiteException {
    public IEnumerable<TableMapping.Column>? Columns { get; }

    public NotNullConstraintViolationException(SQLiteRaw.Result result, string message, TableMapping? mapping, object? obj)
        : base(result, message) {
        if (mapping is not null && obj is not null) {
            Columns = mapping.Columns.Where(column => column.NotNull && column.GetValue(obj) is null);
        }
    }
    public NotNullConstraintViolationException(SQLiteRaw.Result result, string message)
        : this(result, message, null, null) {
    }
    public NotNullConstraintViolationException(SQLiteException exception, TableMapping mapping, object obj)
        : this(exception.Result, exception.Message, mapping, obj) {
    }
}