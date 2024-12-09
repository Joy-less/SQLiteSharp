#pragma warning disable CS1591

using SQLitePCL;

namespace SQLiteSharp;

public static class SqliteRaw {
    public static Result Open(string filename, out Sqlite3DatabaseHandle db, OpenFlags flags, string? vfsName) {
        return (Result)Sqlite3.sqlite3_open_v2(filename, out db, (int)flags, vfsName);
    }
    public static Result Close(Sqlite3DatabaseHandle db) {
        return (Result)Sqlite3.sqlite3_close_v2(db);
    }
    public static Result SetBusyTimeout(Sqlite3DatabaseHandle db, int milliseconds) {
        return (Result)Sqlite3.sqlite3_busy_timeout(db, milliseconds);
    }
    public static int Changes(Sqlite3DatabaseHandle db) {
        return Sqlite3.sqlite3_changes(db);
    }
    public static Sqlite3Statement Prepare(Sqlite3DatabaseHandle db, string query) {
        Result result = (Result)Sqlite3.sqlite3_prepare_v3(db, query, 0, out Sqlite3Statement? statement);
        if (result is not Result.OK) {
            throw new SqliteException(result, GetErrorMessage(db));
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
    public static Result SetExtensionLoadingEnabled(Sqlite3DatabaseHandle db, int onoff) {
        return (Result)Sqlite3.sqlite3_enable_load_extension(db, onoff);
    }
    public static int GetLibraryVersionNumber() {
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
    public static Result SetKey(Sqlite3DatabaseHandle handle, ReadOnlySpan<byte> key, string dbName = "main") {
        return (Result)Sqlite3.sqlite3_key_v2(handle, utf8z.FromString(dbName), key);
    }
    public static Result ChangeKey(Sqlite3DatabaseHandle handle, ReadOnlySpan<byte> key, string dbName = "main") {
        return (Result)Sqlite3.sqlite3_rekey_v2(handle, utf8z.FromString(dbName), key);
    }
    public static Result CreateCollation(Sqlite3DatabaseHandle handle, string name, Func<string, string, int> callback) {
        return (Result)Sqlite3.sqlite3_create_collation(handle, name, null, (object userData, string str1, string str2) => callback(str1, str2));
    }
}

public enum Result {
    OK = Sqlite3.SQLITE_OK,
    Error = Sqlite3.SQLITE_ERROR,
    Internal = Sqlite3.SQLITE_INTERNAL,
    Permissions = Sqlite3.SQLITE_PERM,
    Abort = Sqlite3.SQLITE_ABORT,
    Busy = Sqlite3.SQLITE_BUSY,
    Locked = Sqlite3.SQLITE_LOCKED,
    NoMemory = Sqlite3.SQLITE_NOMEM,
    ReadOnly = Sqlite3.SQLITE_READONLY,
    Interrupt = Sqlite3.SQLITE_INTERRUPT,
    IOError = Sqlite3.SQLITE_IOERR,
    Corrupt = Sqlite3.SQLITE_CORRUPT,
    NotFound = Sqlite3.SQLITE_NOTFOUND,
    Full = Sqlite3.SQLITE_FULL,
    CannotOpen = Sqlite3.SQLITE_CANTOPEN,
    LockError = Sqlite3.SQLITE_PROTOCOL,
    Empty = Sqlite3.SQLITE_EMPTY,
    SchemaChanged = Sqlite3.SQLITE_SCHEMA,
    TooBig = Sqlite3.SQLITE_TOOBIG,
    Constraint = Sqlite3.SQLITE_CONSTRAINT,
    Mismatch = Sqlite3.SQLITE_MISMATCH,
    Misuse = Sqlite3.SQLITE_MISUSE,
    NotImplementedLFS = Sqlite3.SQLITE_NOLFS,
    AccessDenied = Sqlite3.SQLITE_AUTH,
    Format = Sqlite3.SQLITE_FORMAT,
    Range = Sqlite3.SQLITE_RANGE,
    NonDBFile = Sqlite3.SQLITE_NOTADB,
    Notice = Sqlite3.SQLITE_NOTICE,
    Warning = Sqlite3.SQLITE_WARNING,
    Row = Sqlite3.SQLITE_ROW,
    Done = Sqlite3.SQLITE_DONE,
}
public enum ExtendedResult {
    IOErrorRead = Sqlite3.SQLITE_IOERR_READ,
    IOErrorShortRead = Sqlite3.SQLITE_IOERR_SHORT_READ,
    IOErrorWrite = Sqlite3.SQLITE_IOERR_WRITE,
    IOErrorFsync = Sqlite3.SQLITE_IOERR_FSYNC,
    IOErrorDirFSync = Sqlite3.SQLITE_IOERR_DIR_FSYNC,
    IOErrorTruncate = Sqlite3.SQLITE_IOERR_TRUNCATE,
    IOErrorFStat = Sqlite3.SQLITE_IOERR_FSTAT,
    IOErrorUnlock = Sqlite3.SQLITE_IOERR_UNLOCK,
    IOErrorRdlock = Sqlite3.SQLITE_IOERR_RDLOCK,
    IOErrorDelete = Sqlite3.SQLITE_IOERR_DELETE,
    IOErrorBlocked = Sqlite3.SQLITE_IOERR_BLOCKED,
    IOErrorNoMem = Sqlite3.SQLITE_IOERR_NOMEM,
    IOErrorAccess = Sqlite3.SQLITE_IOERR_ACCESS,
    IOErrorCheckReservedLock = Sqlite3.SQLITE_IOERR_CHECKRESERVEDLOCK,
    IOErrorLock = Sqlite3.SQLITE_IOERR_LOCK,
    IOErrorClose = Sqlite3.SQLITE_IOERR_CLOSE,
    IOErrorDirClose = Sqlite3.SQLITE_IOERR_DIR_CLOSE,
    IOErrorSHMOpen = Sqlite3.SQLITE_IOERR_SHMOPEN,
    IOErrorSHMSize = Sqlite3.SQLITE_IOERR_SHMSIZE,
    IOErrorSHMLock = Sqlite3.SQLITE_IOERR_SHMLOCK,
    IOErrorSHMMap = Sqlite3.SQLITE_IOERR_SHMMAP,
    IOErrorSeek = Sqlite3.SQLITE_IOERR_SEEK,
    IOErrorDeleteNoEnt = Sqlite3.SQLITE_IOERR_DELETE_NOENT,
    IOErrorMMap = Sqlite3.SQLITE_IOERR_MMAP,
    LockedSharedcache = Sqlite3.SQLITE_LOCKED_SHAREDCACHE,
    BusyRecovery = Sqlite3.SQLITE_BUSY_RECOVERY,
    CannotOpenNoTempDir = Sqlite3.SQLITE_CANTOPEN_NOTEMPDIR,
    CannotOpenIsDir = Sqlite3.SQLITE_CANTOPEN_ISDIR,
    CannotOpenFullPath = Sqlite3.SQLITE_CANTOPEN_FULLPATH,
    CorruptVTab = Sqlite3.SQLITE_CORRUPT_VTAB,
    ReadonlyRecovery = Sqlite3.SQLITE_READONLY_RECOVERY,
    ReadonlyCannotLock = Sqlite3.SQLITE_READONLY_CANTLOCK,
    ReadonlyRollback = Sqlite3.SQLITE_READONLY_ROLLBACK,
    AbortRollback = Sqlite3.SQLITE_ABORT_ROLLBACK,
    ConstraintCheck = Sqlite3.SQLITE_CONSTRAINT_CHECK,
    ConstraintCommitHook = Sqlite3.SQLITE_CONSTRAINT_COMMITHOOK,
    ConstraintForeignKey = Sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY,
    ConstraintFunction = Sqlite3.SQLITE_CONSTRAINT_FUNCTION,
    ConstraintNotNull = Sqlite3.SQLITE_CONSTRAINT_NOTNULL,
    ConstraintPrimaryKey = Sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY,
    ConstraintTrigger = Sqlite3.SQLITE_CONSTRAINT_TRIGGER,
    ConstraintUnique = Sqlite3.SQLITE_CONSTRAINT_UNIQUE,
    ConstraintVTab = Sqlite3.SQLITE_CONSTRAINT_VTAB,
    NoticeRecoverWAL = Sqlite3.SQLITE_NOTICE_RECOVER_WAL,
    NoticeRecoverRollback = Sqlite3.SQLITE_NOTICE_RECOVER_ROLLBACK,
}

/// <summary>
/// Flags used when accessing a SQLite database, as defined in <see href="https://www.sqlite.org/c3ref/c_open_autoproxy.html">Flags For File Open Operations</see>.
/// </summary>
[Flags]
public enum OpenFlags {
    ReadOnly = Sqlite3.SQLITE_OPEN_READONLY,
    ReadWrite = Sqlite3.SQLITE_OPEN_READWRITE,
    Create = Sqlite3.SQLITE_OPEN_CREATE,
    DeleteOnClose = Sqlite3.SQLITE_OPEN_DELETEONCLOSE,
    Exclusive = Sqlite3.SQLITE_OPEN_EXCLUSIVE,
    AutoProxy = Sqlite3.SQLITE_OPEN_AUTOPROXY,
    Uri = Sqlite3.SQLITE_OPEN_URI,
    Memory = Sqlite3.SQLITE_OPEN_MEMORY,
    MainDb = Sqlite3.SQLITE_OPEN_MAIN_DB,
    TempDb = Sqlite3.SQLITE_OPEN_TEMP_DB,
    TransientDb = Sqlite3.SQLITE_OPEN_TRANSIENT_DB,
    MainJournal = Sqlite3.SQLITE_OPEN_MAIN_JOURNAL,
    TempJournal = Sqlite3.SQLITE_OPEN_TEMP_JOURNAL,
    SubJournal = Sqlite3.SQLITE_OPEN_SUBJOURNAL,
    SuperJournal = Sqlite3.SQLITE_OPEN_MASTER_JOURNAL,
    NoMutex = Sqlite3.SQLITE_OPEN_NOMUTEX,
    FullMutex = Sqlite3.SQLITE_OPEN_FULLMUTEX,
    SharedCache = Sqlite3.SQLITE_OPEN_SHAREDCACHE,
    PrivateCache = Sqlite3.SQLITE_OPEN_PRIVATECACHE,
    Wal = Sqlite3.SQLITE_OPEN_WAL,

    Recommended = Create | ReadWrite | FullMutex | Wal,
}

public enum SqliteType {
    Any = 0,
    Integer = Sqlite3.SQLITE_INTEGER,
    Float = Sqlite3.SQLITE_FLOAT,
    Text = Sqlite3.SQLITE_TEXT,
    Blob = Sqlite3.SQLITE_BLOB,
    Null = Sqlite3.SQLITE_NULL,
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