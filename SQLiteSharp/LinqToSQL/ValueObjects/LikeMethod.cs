namespace SQLiteSharp.LinqToSQL;

/// <summary>
/// An enumeration of the supported string methods for the SQL LIKE statement. The item names should match the related string methods.
/// </summary>
public enum LikeMethod {
    StartsWith,
    EndsWith,
    Contains,
    Equals,
}