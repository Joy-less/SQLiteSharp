﻿using System.Runtime.Serialization;

namespace SQLiteSharp;

/// <summary>
/// On delete/update actions for <see href="https://www.sqlite.org/foreignkeys.html#fk_actions">Foreign Keys</see>.
/// </summary>
public enum ForeignKeyAction {
    /// <summary>
    /// Do nothing.
    /// </summary>
    [EnumMember(Value = "NO ACTION")]
    NoAction,
    /// <summary>
    /// The referenced key is prevented from being changed while it is still referenced.
    /// </summary>
    [EnumMember(Value = "RESTRICT")]
    Restrict,
    /// <summary>
    /// The references are set to null if the referenced key is changed.
    /// </summary>
    [EnumMember(Value = "SET NULL")]
    SetNull,
    /// <summary>
    /// The references are set to their default value if the referenced key is changed.
    /// </summary>
    [EnumMember(Value = "SET DEFAULT")]
    SetDefault,
    /// <summary>
    /// The references are also deleted/updated if the referenced key is changed.
    /// </summary>
    [EnumMember(Value = "CASCADE")]
    Cascade,
}