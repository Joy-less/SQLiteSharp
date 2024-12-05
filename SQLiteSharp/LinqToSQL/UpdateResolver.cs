using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp.LinqToSQL;

public abstract class UpdateResolver {
    public abstract MethodInfo Method { get; }
    public abstract Action<SqlBuilder, MethodCallExpression, object?[]> Resolve { get; }
}

public class StringReplaceResolver : UpdateResolver {
    public override MethodInfo Method {
        get => typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;
    }
    public override void Resolve(SqlBuilder builder, MethodCallExpression callExpression, object?[] arguments) {
        if (arguments.Length != 2) {
            throw new ArgumentException($"REPLACE query requires 2 arguments for replacing old_string with new_string");
        }
        string columnName = LambdaResolver.GetColumnName(callExpression.Object!);
        builder.UpdateColumnReplaceString(columnName, arguments[0], arguments[1]);
    }
}