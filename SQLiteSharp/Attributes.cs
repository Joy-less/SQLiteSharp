namespace SQLiteSharp;


[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class TableAttribute(string? name) : Attribute {
    public string? Name { get; set; } = name;
}

/// <summary>
/// The table will be created without an implicit <c>rowid</c> (see <see href="https://sqlite.org/withoutrowid.html"/>).<br/>
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class WithoutRowIdAttribute() : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ColumnAttribute(string? name) : Attribute {
    public string? Name { get; set; } = name;
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class IgnoreAttribute() : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class PrimaryKeyAttribute() : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class AutoIncrementAttribute() : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
public class IndexAttribute(string? name = null) : Attribute {
    public string? Name { get; set; } = name;
    public virtual bool Unique { get; set; }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class UniqueAttribute() : IndexAttribute {
    public override bool Unique {
        get => true;
        set => throw new InvalidOperationException();
    }
}

/// <summary>
/// The value is not allowed to be null.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class NotNullAttribute() : Attribute {
}

/// <summary>
/// The method of string comparison to use with the value (see <see cref="CollationType"/>).
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class CollationAttribute(string collation) : Attribute {
    public string Value { get; } = collation;
}

/// <summary>
/// A SQL expression which must pass for the column to be valid.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
public class CheckAttribute(string check) : Attribute {
    public string Value { get; } = check;
}