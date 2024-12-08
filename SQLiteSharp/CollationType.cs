namespace SQLiteSharp;

public static class CollationType {
    /// <summary>
    /// Compares the strings for an exact match (case-sensitive).
    /// </summary>
    public const string Binary = "BINARY";
    /// <summary>
    /// Compares the strings with case-insensitive ASCII characters.
    /// </summary>
    public const string NoCase = "NOCASE";
    /// <summary>
    /// Compares the strings, ignoring trailing whitespace.
    /// </summary>
    public const string RTrim = "RTRIM";
    /// <summary>
    /// Compares the strings with case-insensitive unicode characters.
    /// </summary>
    /// <remarks>
    /// This is a custom collation created by <see cref="SQLiteSharp"/>.
    /// </remarks>
    public const string NoCaseInvariant = "NOCASE_INVARIANT";
}