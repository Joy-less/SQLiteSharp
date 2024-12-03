namespace SQLiteSharp;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class TableAttribute(string name) : Attribute {
    public string Name { get; set; } = name;

    /// <summary>
    /// Whether to create the table without <c>rowid</c> (see <see href="https://sqlite.org/withoutrowid.html"/>).<br/>
    /// The default is <see langword="false"/> so that SQLite adds an implicit <c>rowid</c> to every table created.
    /// </summary>
    public bool WithoutRowId { get; set; }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ColumnAttribute(string name) : Attribute {
    public string Name { get; set; } = name;
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class IgnoreAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class PrimaryKeyAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class AutoIncrementAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
public class IndexedAttribute : Attribute {
    public string? Name { get; set; }
    public int Order { get; set; }
    public virtual bool Unique { get; set; }

    public IndexedAttribute() {
    }
    public IndexedAttribute(string name, int order) {
        Name = name;
        Order = order;
    }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class UniqueAttribute : IndexedAttribute {
    public override bool Unique {
        get => true;
        set => throw new InvalidOperationException();
    }
}

/// <summary>
/// The value is not allowed to be null.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class NotNullAttribute : Attribute {
}

/// <summary>
/// The string comparison type to use (see <see cref="CollationType"/>).
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