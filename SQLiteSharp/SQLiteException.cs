namespace SQLiteSharp;

public class SqliteException(Result result, string message) : Exception(message) {
    public Result Result { get; } = result;
}