#nullable disable

namespace SQLiteSharp;

/// <summary>
/// Information about a single column, as returned by <see href="https://www.sqlite.org/pragma.html#pragma_table_info"><c>pragma table_info</c></see>.
/// </summary>
public record ColumnInfo {
    /// <summary>
    /// The index of the column.
    /// </summary>
    [Alias("cid")]
    public int ColumnIndex { get; set; }
    /// <summary>
    /// The name of the column.
    /// </summary>
    [Alias("name")]
    public string Name { get; set; }
    /// <summary>
    /// The exact <see href="https://sqlite.org/datatype3.html">type name</see> of the column, e.g. <c>varchar(20)</c> or <c>INTEGER</c>.
    /// </summary>
    [Alias("type")]
    public string Type { get; set; }
    /// <summary>
    /// Whether the column is allowed to be <see langword="null"/>.
    /// </summary>
    [Alias("notnull")]
    public bool NotNull { get; set; }
    /// <summary>
    /// The default value for the column when inserting rows.
    /// </summary>
    [Alias("dflt_value")]
    public SqliteValue DefaultValue { get; set; }
    /// <summary>
    /// Whether the column is the primary key of the table.
    /// </summary>
    [Alias("pk")]
    public bool PrimaryKey { get; set; }
}