namespace SQLiteSharp;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class TableAttribute(string name) : Attribute {
    public string Name { get; set; } = name;

    /// <summary>
    /// Flag whether to create the table without <c>rowid</c> (see <see href="https://sqlite.org/withoutrowid.html"/>).<br/>
    /// The default is <see langword="false"/> so that SQLite adds an implicit <c>rowid</c> to every table created.
    /// </summary>
    public bool WithoutRowId { get; set; }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ColumnAttribute(string name) : Attribute {
    public string Name { get; set; } = name;
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
public class IgnoreAttribute : Attribute {
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class UniqueAttribute : IndexedAttribute {
    public override bool Unique {
        get => true;
        set => throw new InvalidOperationException();
    }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class MaxLengthAttribute(int length) : Attribute {
    public int Value { get; } = length;
}

/// <summary>
/// Select the collating sequence to use on a column.<br/>
/// <c>BINARY</c>, <c>NOCASE</c>, and <c>RTRIM</c> are supported.<br/>
/// <c>BINARY</c> is the default.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class CollationAttribute(string collation) : Attribute {
    public string Value { get; } = collation;
}

/// <summary>
/// The value is not allowed to be null.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class NotNullAttribute : Attribute {
}

/// <summary>
/// Store the enum by its string name rather than its integer value.
/// </summary>
[AttributeUsage(AttributeTargets.Enum | AttributeTargets.Field)]
public class StoreByNameAttribute : Attribute {
}