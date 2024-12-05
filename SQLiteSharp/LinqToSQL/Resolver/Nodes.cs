using System.Linq.Expressions;

namespace SQLiteSharp.LinqToSQL;

public abstract record Node {
}
public record LikeNode : Node {
    public required LikeMethod Method;
    public required MemberNode MemberNode;
    public required string Value;
}
public record MemberNode : Node {
    public required string TableName;
    public required string FieldName;
}
public record OperationNode : Node {
    public required ExpressionType Operator;
    public required Node Left;
    public required Node Right;
}
public record SingleOperationNode : Node {
    public required ExpressionType Operator;
    public required Node Child;
}
public record ValueNode : Node {
    public required object? Value;
}