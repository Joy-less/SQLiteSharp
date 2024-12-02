using SQLitePCL;

namespace SQLiteSharp;

public static class SQLiteRaw {
    public static Result Open(string filename, out Sqlite3DatabaseHandle db, OpenFlags flags, string? vfsName) {
        return (Result)Sqlite3.sqlite3_open_v2(filename, out db, (int)flags, vfsName);
    }
    public static Result Close(Sqlite3DatabaseHandle db) {
        return (Result)Sqlite3.sqlite3_close_v2(db);
    }
    public static Result BusyTimeout(Sqlite3DatabaseHandle db, int milliseconds) {
        return (Result)Sqlite3.sqlite3_busy_timeout(db, milliseconds);
    }
    public static int Changes(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_changes(db);
    }
    public static Sqlite3Statement Prepare(Sqlite3DatabaseHandle db, string query) {
        int result = Sqlite3.sqlite3_prepare_v2(db, query, out Sqlite3Statement? statement);
        if (result != 0) {
            throw new SQLiteException((Result)result, GetErrorMessage(db));
        }
        return statement;
    }
    public static Result Step(Sqlite3Statement statement) {
        return (Result)Sqlite3.sqlite3_step(statement);
    }
    public static Result Reset(Sqlite3Statement statement) {
        return (Result)Sqlite3.sqlite3_reset(statement);
    }
    public static Result Finalize(Sqlite3Statement statement) {
        return (Result)Sqlite3.sqlite3_finalize(statement);
    }
    public static long GetLastInsertRowId(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_last_insert_rowid(db);
    }
    public static int BindParameterIndex(Sqlite3Statement statement, string name) {
        return Sqlite3.sqlite3_bind_parameter_index(statement, name);
    }
    public static int BindNull(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_bind_null(statement, index);
    }
    public static int BindInt(Sqlite3Statement statement, int index, int value) {
        return Sqlite3.sqlite3_bind_int(statement, index, value);
    }
    public static int BindInt64(Sqlite3Statement statement, int index, long value) {
        return Sqlite3.sqlite3_bind_int64(statement, index, value);
    }
    public static int BindDouble(Sqlite3Statement statement, int index, double value) {
        return Sqlite3.sqlite3_bind_double(statement, index, value);
    }
    public static int BindText(Sqlite3Statement statement, int index, string value) {
        return Sqlite3.sqlite3_bind_text(statement, index, value);
    }
    public static int BindBlob(Sqlite3Statement statement, int index, byte[] value) {
        return Sqlite3.sqlite3_bind_blob(statement, index, value);
    }
    public static int GetColumnCount(Sqlite3Statement statement) {
        return Sqlite3.sqlite3_column_count(statement);
    }
    public static string GetColumnName(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_name(statement, index).utf8_to_string();
    }
    public static SqliteType GetColumnType(Sqlite3Statement statement, int index) {
        return (SqliteType)Sqlite3.sqlite3_column_type(statement, index);
    }
    public static int GetColumnInt(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_int(statement, index);
    }
    public static long GetColumnInt64(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_int64(statement, index);
    }
    public static double GetColumnDouble(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_double(statement, index);
    }
    public static string GetColumnText(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_text(statement, index).utf8_to_string();
    }
    public static byte[] GetColumnBlob(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_blob(statement, index).ToArray();
    }
    public static int GetColumnByteCount(Sqlite3Statement statement, int index) {
        return Sqlite3.sqlite3_column_bytes(statement, index);
    }
    public static SqliteValue GetColumnValue(Sqlite3Statement statement, int index) {
        return GetColumnType(statement, index) switch {
            SqliteType.Integer => GetColumnInt64(statement, index),
            SqliteType.Float => GetColumnDouble(statement, index),
            SqliteType.Text => GetColumnText(statement, index),
            SqliteType.Blob => GetColumnBlob(statement, index),
            SqliteType.Null => SqliteValue.Null,
            _ => throw new NotImplementedException()
        };
    }
    public static Result EnableLoadExtension(Sqlite3DatabaseHandle db, int onoff) {
        return (Result)Sqlite3.sqlite3_enable_load_extension(db, onoff);
    }
    public static int LibVersionNumber() {
        return Sqlite3.sqlite3_libversion_number();
    }
    public static Result GetResult(Sqlite3DatabaseHandle db) {
        return (Result)Sqlite3.sqlite3_errcode(db);
    }
    public static ExtendedResult GetExtendedErrorCode(Sqlite3DatabaseHandle db) {
        return (ExtendedResult)Sqlite3.sqlite3_extended_errcode(db);
    }
    public static string GetErrorMessage(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_errmsg(db).utf8_to_string();
    }
    public static Sqlite3BackupHandle BackupInit(Sqlite3DatabaseHandle destDb, string destName, Sqlite3DatabaseHandle sourceDb, string sourceName) {
        return Sqlite3.sqlite3_backup_init(destDb, destName, sourceDb, sourceName);
    }
    public static Result BackupStep(Sqlite3BackupHandle backup, int numPages) {
        return (Result)Sqlite3.sqlite3_backup_step(backup, numPages);
    }
    public static Result BackupFinish(Sqlite3BackupHandle backup) {
        return (Result)Sqlite3.sqlite3_backup_finish(backup);
    }
    public static Result SetKey(Sqlite3DatabaseHandle handle, ReadOnlySpan<byte> key, string name = "main") {
        return (Result)Sqlite3.sqlite3_key_v2(handle, utf8z.FromString(name), key);
    }
    public static Result ChangeKey(Sqlite3DatabaseHandle handle, ReadOnlySpan<byte> key, string name = "main") {
        return (Result)Sqlite3.sqlite3_rekey_v2(handle, utf8z.FromString(name), key);
    }
}

public enum Result : int {
    OK = 0,
    Error = 1,
    Internal = 2,
    Perm = 3,
    Abort = 4,
    Busy = 5,
    Locked = 6,
    NoMem = 7,
    ReadOnly = 8,
    Interrupt = 9,
    IOError = 10,
    Corrupt = 11,
    NotFound = 12,
    Full = 13,
    CannotOpen = 14,
    LockErr = 15,
    Empty = 16,
    SchemaChngd = 17,
    TooBig = 18,
    Constraint = 19,
    Mismatch = 20,
    Misuse = 21,
    NotImplementedLFS = 22,
    AccessDenied = 23,
    Format = 24,
    Range = 25,
    NonDBFile = 26,
    Notice = 27,
    Warning = 28,
    Row = 100,
    Done = 101
}

public enum ExtendedResult : int {
    IOErrorRead = Result.IOError | (1 << 8),
    IOErrorShortRead = Result.IOError | (2 << 8),
    IOErrorWrite = Result.IOError | (3 << 8),
    IOErrorFsync = Result.IOError | (4 << 8),
    IOErrorDirFSync = Result.IOError | (5 << 8),
    IOErrorTruncate = Result.IOError | (6 << 8),
    IOErrorFStat = Result.IOError | (7 << 8),
    IOErrorUnlock = Result.IOError | (8 << 8),
    IOErrorRdlock = Result.IOError | (9 << 8),
    IOErrorDelete = Result.IOError | (10 << 8),
    IOErrorBlocked = Result.IOError | (11 << 8),
    IOErrorNoMem = Result.IOError | (12 << 8),
    IOErrorAccess = Result.IOError | (13 << 8),
    IOErrorCheckReservedLock = Result.IOError | (14 << 8),
    IOErrorLock = Result.IOError | (15 << 8),
    IOErrorClose = Result.IOError | (16 << 8),
    IOErrorDirClose = Result.IOError | (17 << 8),
    IOErrorSHMOpen = Result.IOError | (18 << 8),
    IOErrorSHMSize = Result.IOError | (19 << 8),
    IOErrorSHMLock = Result.IOError | (20 << 8),
    IOErrorSHMMap = Result.IOError | (21 << 8),
    IOErrorSeek = Result.IOError | (22 << 8),
    IOErrorDeleteNoEnt = Result.IOError | (23 << 8),
    IOErrorMMap = Result.IOError | (24 << 8),
    LockedSharedcache = Result.Locked | (1 << 8),
    BusyRecovery = Result.Busy | (1 << 8),
    CannottOpenNoTempDir = Result.CannotOpen | (1 << 8),
    CannotOpenIsDir = Result.CannotOpen | (2 << 8),
    CannotOpenFullPath = Result.CannotOpen | (3 << 8),
    CorruptVTab = Result.Corrupt | (1 << 8),
    ReadonlyRecovery = Result.ReadOnly | (1 << 8),
    ReadonlyCannotLock = Result.ReadOnly | (2 << 8),
    ReadonlyRollback = Result.ReadOnly | (3 << 8),
    AbortRollback = Result.Abort | (2 << 8),
    ConstraintCheck = Result.Constraint | (1 << 8),
    ConstraintCommitHook = Result.Constraint | (2 << 8),
    ConstraintForeignKey = Result.Constraint | (3 << 8),
    ConstraintFunction = Result.Constraint | (4 << 8),
    ConstraintNotNull = Result.Constraint | (5 << 8),
    ConstraintPrimaryKey = Result.Constraint | (6 << 8),
    ConstraintTrigger = Result.Constraint | (7 << 8),
    ConstraintUnique = Result.Constraint | (8 << 8),
    ConstraintVTab = Result.Constraint | (9 << 8),
    NoticeRecoverWAL = Result.Notice | (1 << 8),
    NoticeRecoverRollback = Result.Notice | (2 << 8),
}

/// <summary>
/// Flags used when accessing a SQLite database, as defined in <see href="https://www.sqlite.org/c3ref/c_open_autoproxy.html">Flags For File Open Operations</see>.
/// </summary>
[Flags]
public enum OpenFlags {
    ReadOnly = 0x00000001,
    ReadWrite = 0x00000002,
    Create = 0x00000004,
    DeleteOnClose = 0x00000008,
    Exclusive = 0x00000010,
    AutoProxy = 0x00000020,
    Uri = 0x00000040,
    Memory = 0x00000080,
    MainDb = 0x00000100,
    TempDb = 0x00000200,
    TransientDb = 0x00000400,
    MainJournal = 0x00000800,
    TempJournal = 0x00001000,
    SubJournal = 0x00002000,
    SuperJournal = 0x00004000,
    NoMutex = 0x00008000,
    FullMutex = 0x00010000,
    SharedCache = 0x00020000,
    PrivateCache = 0x00040000,
    Wal = 0x00080000,
    NoFollow = 0x01000000,
    ExresCode = 0x02000000,

    Recommended = Create | ReadWrite | FullMutex | Wal,
}

public enum SqliteType : int {
    Integer = 1,
    Float = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

public readonly struct SqliteValue {
    public static SqliteValue Null { get; } = new();

    public SqliteType SqliteType { get; } = SqliteType.Null;

    private readonly long? Integer;
    private readonly double? Float;
    private readonly string? Text;
    private readonly byte[]? Blob;

    private SqliteValue(long? @integer) {
        SqliteType = SqliteType.Integer;
        Integer = @integer;
    }
    private SqliteValue(double? @float) {
        SqliteType = SqliteType.Float;
        Float = @float;
    }
    private SqliteValue(string? @text) {
        SqliteType = SqliteType.Text;
        Text = @text;
    }
    private SqliteValue(byte[]? @blob) {
        SqliteType = SqliteType.Blob;
        Blob = @blob;
    }

    public object? AsObject => SqliteType switch {
        SqliteType.Integer => Integer,
        SqliteType.Float => Float,
        SqliteType.Text => Text,
        SqliteType.Blob => Blob,
        SqliteType.Null or _ => null,
    };
    public long AsInteger => (long)AsObject!;
    public double AsFloat => (double)AsObject!;
    public string AsText => (string)AsObject!;
    public byte[] AsBlob => (byte[])AsObject!;

    public static implicit operator SqliteValue(long? value) => new(value);
    public static implicit operator SqliteValue(double? value) => new(value);
    public static implicit operator SqliteValue(string? value) => new(value);
    public static implicit operator SqliteValue(byte[]? value) => new(value);
}