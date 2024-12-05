using System.Linq.Expressions;

namespace SQLiteSharp.LinqToSQL;

internal abstract record Node {
}
internal record LikeNode : Node {
    public required LikeMethod Method;
    public required MemberNode MemberNode;
    public required string Value;
}
internal record MemberNode : Node {
    public required string TableName;
    public required string FieldName;
}
internal record OperationNode : Node {
    public required ExpressionType Operator;
    public required Node Left;
    public required Node Right;
}
internal record SingleOperationNode : Node {
    public required ExpressionType Operator;
    public required Node Child;
}
internal record ValueNode : Node {
    public required object? Value;
}