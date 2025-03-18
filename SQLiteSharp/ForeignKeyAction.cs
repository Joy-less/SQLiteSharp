using System.Runtime.Serialization;

namespace SQLiteSharp;

/// <summary>
/// On delete/update actions for <see href="https://www.sqlite.org/foreignkeys.html#fk_actions">Foreign Keys</see>.
/// </summary>
public enum ForeignKeyAction {
    /// <summary>
    /// Do nothing.
    /// </summary>
    [EnumMember(Value = "no action")]
    NoAction,
    /// <summary>
    /// The referenced key is prevented from being changed while it is still referenced.
    /// </summary>
    [EnumMember(Value = "restrict")]
    Restrict,
    /// <summary>
    /// The references are set to null if the referenced key is changed.
    /// </summary>
    [EnumMember(Value = "set null")]
    SetNull,
    /// <summary>
    /// The references are set to their default value if the referenced key is changed.
    /// </summary>
    [EnumMember(Value = "set default")]
    SetDefault,
    /// <summary>
    /// The references are also deleted/updated if the referenced key is changed.
    /// </summary>
    [EnumMember(Value = "cascade")]
    Cascade,
}