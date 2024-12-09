#nullable disable

namespace SQLiteSharp;

/// <summary>
/// Information about a single table, as returned by <see href="https://www.sqlite.org/pragma.html#pragma_table_list"><c>pragma table_list</c></see>.
/// </summary>
public record TableInfo {
    /// <summary>
    /// The schema in which the table appears (e.g. "main" or "temp").
    /// </summary>
    [Alias("schema")] public string Schema { get; set; }
    /// <summary>
    /// The name of the table.
    /// </summary>
    [Alias("name")] public string Name { get; set; }
    /// <summary>
    /// The type of object (e.g. "table", "view", "shadow" or "virtual").
    /// </summary>
    [Alias("type")] public string Type { get; set; }
    /// <summary>
    /// The number of columns in the table, including generated and hidden columns.
    /// </summary>
    [Alias("ncol")] public int ColumnCount { get; set; }
    /// <summary>
    /// Whether the table was created without an implicit row ID (see <see href="https://sqlite.org/withoutrowid.html"/>).
    /// </summary>
    [Alias("wr")] public bool WithoutRowId { get; set; }
}