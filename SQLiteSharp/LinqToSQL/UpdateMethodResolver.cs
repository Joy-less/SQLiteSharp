using System.Linq.Expressions;
using System.Reflection;

namespace SQLiteSharp.LinqToSQL;

internal abstract class UpdateMethodResolver {
    public abstract Type Type { get; }
    public abstract string Name { get; }
    public abstract Type[] Arguments { get; }
    public abstract void Resolve(SqlQueryBuilder builder, MethodCallExpression callExpression, object?[] arguments);

    public MethodInfo Method => Type.GetMethod(Name, Arguments)!;
}
internal class StringReplaceResolver : UpdateMethodResolver {
    public override Type Type => typeof(string);
    public override string Name => nameof(string.Replace);
    public override Type[] Arguments => [typeof(string), typeof(string)];

    public override void Resolve(SqlQueryBuilder builder, MethodCallExpression callExpression, object?[] arguments) {
        if (arguments.Length != 2) {
            throw new ArgumentException($"REPLACE query requires 2 arguments for replacing old_string with new_string");
        }
        string columnName = LambdaResolver.GetColumnName(callExpression.Object!);
        builder.UpdateColumnReplaceString(columnName, arguments[0], arguments[1]);
    }
}