global using Sqlite3 = SQLitePCL.raw;
global using Sqlite3BackupHandle = SQLitePCL.sqlite3_backup;
global using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;
global using Sqlite3Statement = SQLitePCL.sqlite3_stmt;

using System.Linq.Expressions;

namespace SQLiteSharp;

public static class Globals {
    /// <summary>
    /// Convert an input string to a quoted SQL string that can be safely used in queries.<br/>
    /// For example, (<c>red "blue" green</c>) becomes (<c>"red ""blue"" green"</c>).
    /// </summary>
    public static string SqlQuote(this string? unsafeString) {
        if (unsafeString is null) {
            return "null";
        }
        return $"\"{unsafeString.Replace("\"", "\"\"")}\"";
    }
    public static Expression? AndAlso(this Expression? left, Expression? right) {
        if (left is not null && right is not null) {
            return Expression.AndAlso(left, right);
        }
        else if (left is not null) {
            return left;
        }
        else if (right is not null) {
            return right;
        }
        else {
            return null;
        }
    }
    public static Type AsNotNullable(this Type Type) {
        return Nullable.GetUnderlyingType(Type) ?? Type;
    }
}