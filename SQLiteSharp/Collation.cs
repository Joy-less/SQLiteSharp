namespace SQLiteSharp;

/// <summary>
/// Contains names for string comparison collations built into <see cref="SQLiteSharp"/>.
/// </summary>
public static class Collation {
    /// <summary>
    /// Compares the strings for an exact match (case-sensitive).
    /// </summary>
    /// <remarks>
    /// This is similar to <see cref="StringComparison.Ordinal"/>.
    /// </remarks>
    public const string Binary = "BINARY";
    /// <summary>
    /// Compares the strings with case-insensitive ASCII characters.
    /// </summary>
    /// <remarks>
    /// This is similar to <see cref="StringComparison.OrdinalIgnoreCase"/>.
    /// </remarks>
    public const string NoCase = "NOCASE";
    /// <summary>
    /// Compares the strings for an exact match, ignoring trailing whitespace.
    /// </summary>
    public const string RTrim = "RTRIM";
    /// <summary>
    /// Compares the strings with case-sensitive unicode characters.
    /// </summary>
    /// <remarks>
    /// This is a custom collation created automatically and corresponds to <see cref="StringComparison.InvariantCulture"/>.
    /// </remarks>
    public const string Invariant = "INVARIANT";
    /// <summary>
    /// Compares the strings with case-insensitive unicode characters.
    /// </summary>
    /// <remarks>
    /// This is a custom collation created automatically and corresponds to <see cref="StringComparison.InvariantCultureIgnoreCase"/>.
    /// </remarks>
    public const string Invariant_NoCase = "INVARIANT_NOCASE";
}