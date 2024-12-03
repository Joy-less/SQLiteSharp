using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLiteSharp;

public record struct SqlExpression {
    public string? CommandText { get; set; }
    public object? Value { get; set; }

    public static SqlExpression FromExpression(Expression expression, IList<object?> parameters, Func<string, string> memberNameToColumnName) {
        if (expression is BinaryExpression binaryExpression) {
            // VB turns 'x=="foo"' into 'CompareString(x,"foo",true/false)==0', so we need to unwrap it
            // http://blogs.msdn.com/b/vbteam/archive/2007/09/18/vb-expression-trees-string-comparisons.aspx
            if (binaryExpression.Left is MethodCallExpression leftCall) {
                if (leftCall.Method.DeclaringType?.FullName == "Microsoft.VisualBasic.CompilerServices.Operators" && leftCall.Method.Name == "CompareString") {
                    binaryExpression = Expression.MakeBinary(binaryExpression.NodeType, leftCall.Arguments[0], leftCall.Arguments[1]);
                }
            }

            SqlExpression leftResult = FromExpression(binaryExpression.Left, parameters, memberNameToColumnName);
            SqlExpression rightResult = FromExpression(binaryExpression.Right, parameters, memberNameToColumnName);

            // If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
            string text;
            if (leftResult.CommandText == "?" && leftResult.Value == null) {
                text = CompileNullBinaryExpression(binaryExpression, rightResult);
            }
            else if (rightResult.CommandText == "?" && rightResult.Value == null) {
                text = CompileNullBinaryExpression(binaryExpression, leftResult);
            }
            else {
                text = "(" + leftResult.CommandText + " " + GetSqlOperator(binaryExpression.NodeType) + " " + rightResult.CommandText + ")";
            }
            return new SqlExpression() {
                CommandText = text
            };
        }
        else if (expression.NodeType is ExpressionType.Not) {
            Expression operandExpression = ((UnaryExpression)expression).Operand;
            SqlExpression operand = FromExpression(operandExpression, parameters, memberNameToColumnName);
            object? value = operand.Value;
            if (value is bool boolValue) {
                value = !boolValue;
            }
            return new SqlExpression() {
                CommandText = "not(" + operand.CommandText + ")",
                Value = value
            };
        }
        else if (expression.NodeType is ExpressionType.Call) {
            MethodCallExpression call = (MethodCallExpression)expression;
            SqlExpression callTarget = call.Object is not null ? FromExpression(call.Object, parameters, memberNameToColumnName) : default;

            SqlExpression[] callArguments = new SqlExpression[call.Arguments.Count];
            for (int i = 0; i < callArguments.Length; i++) {
                callArguments[i] = FromExpression(call.Arguments[i], parameters, memberNameToColumnName);
            }

            string? sqlCall = null;

            if (call.Method.Name is "Like" && callArguments.Length == 2) {
                sqlCall = "(" + callArguments[0].CommandText + " like " + callArguments[1].CommandText + ")";
            }
            else if (call.Method.Name is "Contains" && callArguments.Length == 2) {
                // string.Contains(string, StringComparison)
                if (call.Object?.Type == typeof(string)) {
                    StringComparison comparison = (StringComparison)callArguments[1].Value!;
                    switch (comparison) {
                        case StringComparison.Ordinal:
                        case StringComparison.CurrentCulture:
                            sqlCall = "( instr(" + callTarget.CommandText + "," + callArguments[0].CommandText + ") >0 )";
                            break;
                        case StringComparison.OrdinalIgnoreCase:
                        case StringComparison.CurrentCultureIgnoreCase:
                            sqlCall = "(" + callTarget.CommandText + " like ( '%' || " + callArguments[0].CommandText + " || '%'))";
                            break;
                    }
                }
                else {
                    sqlCall = "(" + callArguments[1].CommandText + " in " + callArguments[0].CommandText + ")";
                }
            }
            else if (call.Method.Name is "Contains" && callArguments.Length == 1) {
                if (call.Object is not null && call.Object.Type == typeof(string)) {
                    sqlCall = "( instr(" + callTarget.CommandText + "," + callArguments[0].CommandText + ") >0 )";
                }
                else {
                    sqlCall = "(" + callArguments[0].CommandText + " in " + callTarget.CommandText + ")";
                }
            }
            else if (call.Method.Name is "StartsWith" && callArguments.Length >= 1) {
                StringComparison comparisonType = StringComparison.CurrentCulture;
                if (callArguments.Length == 2) {
                    comparisonType = (StringComparison)callArguments[1].Value!;
                }
                switch (comparisonType) {
                    case StringComparison.Ordinal or StringComparison.CurrentCulture:
                        sqlCall = "( substr(" + callTarget.CommandText + ", 1, " + callArguments[0].Value!.ToString()!.Length + ") =  " + callArguments[0].CommandText + ")";
                        break;
                    case StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase:
                        sqlCall = "(" + callTarget.CommandText + " like (" + callArguments[0].CommandText + " || '%'))";
                        break;
                }
            }
            else if (call.Method.Name is "EndsWith" && callArguments.Length >= 1) {
                StringComparison comparisonType = StringComparison.CurrentCulture;
                if (callArguments.Length == 2) {
                    comparisonType = (StringComparison)callArguments[1].Value!;
                }
                switch (comparisonType) {
                    case StringComparison.Ordinal or StringComparison.CurrentCulture:
                        sqlCall = "( substr(" + callTarget.CommandText + ", length(" + callTarget.CommandText + ") - " + callArguments[0].Value!.ToString()!.Length + "+1, " + callArguments[0].Value!.ToString()!.Length + ") =  " + callArguments[0].CommandText + ")";
                        break;
                    case StringComparison.OrdinalIgnoreCase or StringComparison.CurrentCultureIgnoreCase:
                        sqlCall = "(" + callTarget.CommandText + " like ('%' || " + callArguments[0].CommandText + "))";
                        break;
                }
            }
            else if (call.Method.Name is "Equals" && callArguments.Length == 1) {
                sqlCall = "(" + callTarget.CommandText + " = (" + callArguments[0].CommandText + "))";
            }
            else if (call.Method.Name is "ToLower") {
                sqlCall = "(lower(" + callTarget.CommandText + "))";
            }
            else if (call.Method.Name is "ToUpper") {
                sqlCall = "(upper(" + callTarget.CommandText + "))";
            }
            else if (call.Method.Name is "Replace" && callArguments.Length == 2) {
                sqlCall = "(replace(" + callTarget.CommandText + "," + callArguments[0].CommandText + "," + callArguments[1].CommandText + "))";
            }
            else if (call.Method.Name is "IsNullOrEmpty" && callArguments.Length == 1) {
                sqlCall = "(" + callArguments[0].CommandText + " is null or " + callArguments[0].CommandText + " = '' )";
            }
            else {
                sqlCall = call.Method.Name.ToLower() + "(" + string.Join(",", callArguments.Select(callArgument => callArgument.CommandText)) + ")";
            }

            return new SqlExpression() {
                CommandText = sqlCall
            };
        }
        else if (expression.NodeType is ExpressionType.Constant) {
            ConstantExpression constantExpression = (ConstantExpression)expression;
            parameters.Add(constantExpression.Value);
            return new SqlExpression() {
                CommandText = "?",
                Value = constantExpression.Value
            };
        }
        else if (expression.NodeType is ExpressionType.Convert) {
            UnaryExpression unaryExpression = (UnaryExpression)expression;
            SqlExpression valueResult = FromExpression(unaryExpression.Operand, parameters, memberNameToColumnName);
            return new SqlExpression() {
                CommandText = valueResult.CommandText,
                Value = valueResult.Value is not null ? ConvertTo(valueResult.Value, unaryExpression.Type) : null
            };
        }
        else if (expression.NodeType is ExpressionType.MemberAccess) {
            MemberExpression memberExpression = (MemberExpression)expression;

            ParameterExpression? parameterExpression = memberExpression.Expression as ParameterExpression;
            if (parameterExpression is null) {
                if (memberExpression.Expression is UnaryExpression convert && convert.NodeType is ExpressionType.Convert) {
                    parameterExpression = convert.Operand as ParameterExpression;
                }
            }

            if (parameterExpression is not null) {
                // This is a column of our table, output just the column name
                // Need to translate it if that column name is mapped
                string columnName = memberNameToColumnName(memberExpression.Member.Name);
                return new SqlExpression() {
                    CommandText = columnName.SqlQuote()
                };
            }
            else {
                object? memberTarget = null;
                if (memberExpression.Expression != null) {
                    SqlExpression result = FromExpression(memberExpression.Expression, parameters, memberNameToColumnName);
                    if (result.Value is null) {
                        throw new NotSupportedException("Member access failed to compile expression");
                    }
                    if (result.CommandText is "?") {
                        parameters.RemoveAt(parameters.Count - 1);
                    }
                    memberTarget = result.Value;
                }

                // Get the member value
                object? memberValue = memberExpression.Member switch {
                    PropertyInfo propertyInfo => propertyInfo.GetValue(memberTarget),
                    FieldInfo fieldInfo => fieldInfo.GetValue(memberTarget),
                    _ => throw new NotSupportedException($"MemberExpression: {memberExpression.Member.GetType()}")
                };

                // Work special magic for enumerables
                if (memberValue is IEnumerable and not (string or IEnumerable<byte>)) {
                    StringBuilder builder = new();
                    builder.Append('(');
                    string comma = "";
                    foreach (object item in (IEnumerable)memberValue) {
                        parameters.Add(item);
                        builder.Append(comma);
                        builder.Append('?');
                        comma = ",";
                    }
                    builder.Append(')');
                    return new SqlExpression() {
                        CommandText = builder.ToString(),
                        Value = memberValue
                    };
                }
                else {
                    parameters.Add(memberValue);
                    return new SqlExpression() {
                        CommandText = "?",
                        Value = memberValue
                    };
                }
            }
        }

        // Invalid expression
        throw new NotSupportedException($"Cannot convert Linq to SQL: '{expression.NodeType}'");
    }

    private static object? ConvertTo(object? obj, Type type) {
        return Convert.ChangeType(obj, Nullable.GetUnderlyingType(type) ?? type);
    }
    /// <summary>
    /// Compiles a BinaryExpression where one of the parameters is null.
    /// </summary>
    /// <param name="expression">The expression to compile</param>
    /// <param name="parameter">The non-null parameter</param>
    private static string CompileNullBinaryExpression(BinaryExpression expression, SqlExpression parameter) {
        if (expression.NodeType is ExpressionType.Equal) {
            return $"({parameter.CommandText} is ?)";
        }
        else if (expression.NodeType is ExpressionType.NotEqual) {
            return $"({parameter.CommandText} is not ?)";
        }
        else if (expression.NodeType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual) {
            return $"({parameter.CommandText} < ?)"; // always false
        }
        else {
            throw new NotSupportedException($"Cannot compile Null-BinaryExpression with type {expression.NodeType}");
        }
    }
    private static string GetSqlOperator(ExpressionType expressionType) {
        return expressionType switch {
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.And => "&",
            ExpressionType.AndAlso => "and",
            ExpressionType.Or => "|",
            ExpressionType.OrElse => "or",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            _ => throw new NotSupportedException($"Cannot get SQL operator for {expressionType}")
        };
    }
}