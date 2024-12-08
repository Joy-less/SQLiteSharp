global using Sqlite3 = SQLitePCL.raw;
global using Sqlite3BackupHandle = SQLitePCL.sqlite3_backup;
global using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;
global using Sqlite3Statement = SQLitePCL.sqlite3_stmt;

using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp;

public static class Globals {
    /// <summary>
    /// Convert an input string to a quoted SQL string that can be safely used in queries.<br/>
    /// For example, (<c>red "blue" green</c>) becomes (<c>"red ""blue"" green"</c>).
    /// </summary>
    public static string SqlQuote(this string? unsafeString, string QuoteChar = "\"") {
        if (unsafeString is null) {
            return "null";
        }
        return $"{QuoteChar}{unsafeString.Replace(QuoteChar, $"{QuoteChar}{QuoteChar}")}{QuoteChar}";
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
    public static object? GetValue(this MemberInfo memberInfo, object? obj) {
        return memberInfo switch {
            PropertyInfo propertyInfo => propertyInfo.GetValue(obj),
            FieldInfo fieldInfo => fieldInfo.GetValue(obj),
            _ => throw new ArgumentException(null, nameof(memberInfo)),
        };
    }
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