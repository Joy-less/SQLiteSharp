namespace SQLiteSharp.LinqToSQL;

/// <summary>
/// An enumeration of the supported aggregate SQL functions. The item names should match the related function names
/// </summary>
public enum SelectFunction {
    COUNT,
    DISTINCT,
    SUM,
    MIN,
    MAX,
    AVG,
}