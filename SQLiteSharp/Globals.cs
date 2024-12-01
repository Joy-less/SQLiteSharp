global using Sqlite3 = SQLitePCL.raw;
global using Sqlite3BackupHandle = SQLitePCL.sqlite3_backup;
global using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;
global using Sqlite3Statement = SQLitePCL.sqlite3_stmt;

global using static SQLiteSharp.Globals;

namespace SQLiteSharp;

public static class Globals {
    /// <summary>
    /// Convert an input string to a quoted SQL string that can be safely used in queries.<br/>
    /// For example, <c>red 'blue' green</c> becomes <c>'red ''blue'' green'</c>.
    /// </summary>
    public static string Quote(string? unsafeString) {
        if (unsafeString is null) {
            return "null";
        }
        return $"'{unsafeString.Replace("'", "''")}'";
    }
}