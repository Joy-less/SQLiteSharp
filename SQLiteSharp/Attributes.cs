namespace SQLiteSharp;

/// <summary>
/// The table/column will be mapped with the given name.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Property | AttributeTargets.Field)]
public class AliasAttribute(string? name) : Attribute {
    /// <summary>
    /// The table/column name to store in the database.
    /// </summary>
    public string? Name { get; set; } = name;
}

/// <summary>
/// The table will be created without an implicit <c>rowid</c> (see <see href="https://sqlite.org/withoutrowid.html"/>).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class WithoutRowIdAttribute() : Attribute {
}

/// <summary>
/// The member will not be treated as a column in the table.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class IgnoreAttribute() : Attribute {
}

/// <summary>
/// The column will be designated as the primary key (ID) for the table.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class PrimaryKeyAttribute() : Attribute {
}

/// <summary>
/// The column will be designated as a foreign key (ID) for another table.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class ForeignKeyAttribute(string foreignTable, string foreignColumn) : Attribute {
    /// <summary>
    /// The name of the foreign table.
    /// </summary>
    public string ForeignTable { get; set; } = foreignTable;
    /// <summary>
    /// The name of the column in the foreign table.
    /// </summary>
    public string ForeignColumn { get; set; } = foreignColumn;
    /// <summary>
    /// The action to complete when the row referenced by the foreign key is deleted.
    /// </summary>
    public ForeignKeyAction OnDelete { get; set; } = ForeignKeyAction.NoAction;
    /// <summary>
    /// The action to complete when the foreign key is changed.
    /// </summary>
    public ForeignKeyAction OnUpdate { get; set; } = ForeignKeyAction.NoAction;
}

/// <summary>
/// An <see href="https://www.sqlite.org/lang_createindex.html">index</see> will be created for the column.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
public class IndexAttribute(string? name = null) : Attribute {
    /// <summary>
    /// The name of the index to create.
    /// </summary>
    /// <remarks>
    /// An index can be created over multiple columns by setting this to the same value.
    /// </remarks>
    public string? Name { get; set; } = name;
    /// <summary>
    /// If <see langword="true"/>, every row must have a unique value for this index.<br/>
    /// Default: <see langword="true"/>
    /// </summary>
    public bool Unique { get; set; }
}

/// <summary>
/// The column will be automatically incremented from the last value when inserting in the table.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class AutoIncrementAttribute() : Attribute {
}

/// <summary>
/// A unique constraint will be created for this column.
/// </summary>
/// <remarks>
/// An index is internally created for this column by SQLite.
/// </remarks>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class UniqueAttribute() : Attribute {
}

/// <summary>
/// The value is not allowed to be null.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class NotNullAttribute() : Attribute {
}

/// <summary>
/// The method of string comparison to use with the value (see <see cref="Collation"/>).
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class CollationAttribute(string collation) : Attribute {
    /// <summary>
    /// The name of the collation.
    /// </summary>
    public string Value { get; } = collation;
}

/// <summary>
/// A SQL expression which must pass for the column to be valid.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
public class CheckAttribute(string check) : Attribute {
    /// <summary>
    /// The SQL expression to execute.
    /// </summary>
    public string Value { get; } = check;
}