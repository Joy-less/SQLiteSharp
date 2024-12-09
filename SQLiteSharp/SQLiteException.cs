namespace SQLiteSharp;

/// <summary>
/// An error returned from the native SQLite PCL.
/// </summary>
public class SqliteException(Result result, string message) : Exception(message) {
    /// <summary>
    /// The result code for the error.
    /// </summary>
    public Result Result { get; } = result;
}