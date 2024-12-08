namespace SQLiteSharp;

public record ColumnInfo {
    [Column("cid")] public int ColumnId { get; set; }
    [Column("name")] public string Name { get; set; } = null!;
    [Column("type")] public string Type { get; set; } = null!;
    [Column("notnull")] public bool NotNull { get; set; }
    [Column("pk")] public int PrimaryKey { get; set; }
}