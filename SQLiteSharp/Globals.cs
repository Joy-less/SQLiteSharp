global using Sqlite3 = SQLitePCL.raw;
global using Sqlite3BackupHandle = SQLitePCL.sqlite3_backup;
global using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;
global using Sqlite3Statement = SQLitePCL.sqlite3_stmt;

using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp;

/// <summary>
/// Extension methods used in <see cref="SQLiteSharp"/>.
/// </summary>
public static class Globals {
    /// <summary>
    /// Converts an input string to a quoted SQL string that can be safely used in queries.<br/>
    /// For example, (<c>red "blue" green</c>) becomes (<c>"red ""blue"" green"</c>).
    /// </summary>
    /// <remarks>
    /// The default quote character is only suitable for identifiers. String literals should use "<c>'</c>".
    /// </remarks>
    public static string SqlQuote(this string? unsafeString, string QuoteChar = "\"") {
        if (unsafeString is null) {
            return "null";
        }
        return $"{QuoteChar}{unsafeString.Replace(QuoteChar, $"{QuoteChar}{QuoteChar}")}{QuoteChar}";
    }
    /// <summary>
    /// Gets the value of the member if it's a property or field.
    /// </summary>
    public static object? GetValue(this MemberInfo memberInfo, object? obj) {
        return memberInfo switch {
            PropertyInfo propertyInfo => propertyInfo.GetValue(obj),
            FieldInfo fieldInfo => fieldInfo.GetValue(obj),
            _ => throw new ArgumentException(null, nameof(memberInfo)),
        };
    }
    /// <summary>
    /// Sets the value of the member if it's a property or field.
    /// </summary>
    public static void SetValue(this MemberInfo memberInfo, object? obj, object? value) {
        switch (memberInfo) {
            case PropertyInfo propertyInfo:
                propertyInfo.SetValue(obj, value);
                break;
            case FieldInfo fieldInfo:
                fieldInfo.SetValue(obj, value);
                break;
            default:
                throw new ArgumentException(null, nameof(memberInfo));
        }
    }
    /// <summary>
    /// Gets the constant value of the expression or compiles it to a delegate and invokes it.
    /// </summary>
    public static object? Execute(this Expression? expression) {
        if (expression is null) {
            return null;
        }
        else if (expression is ConstantExpression constantExpression) {
            return constantExpression.Value;
        }
        else {
            return Expression.Lambda(expression).Compile().DynamicInvoke();
        }
    }
}