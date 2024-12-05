namespace SQLiteSharp.LinqToSQL;

internal static class Adapter {
    public static string QueryStringPage(string source, string selection, string conditions, string order, int pageSize, int pageIndex = 0) {
        if (pageIndex == 0) {
            return $"SELECT TOP({pageSize}) {selection} FROM {source} {conditions} {order}";
        }
        return $"SELECT {selection} FROM {source} {conditions} {order} OFFSET {pageSize * pageIndex} ROWS FETCH NEXT {pageSize} ROWS ONLY";
    }
    /*public static string Table(string tableName) {
        return $"[{tableName}]";
    }
    public static  string Field(string fieldName) {
        return $"[{fieldName}]";
    }
    public static string Field(string tableName, string fieldName) {
        return $"[{tableName}].[{fieldName}]";
    }
    public static string Parameter(string parameterId) {
        return "@" + parameterId;
    }*/
    public static string QueryString(string selection, string source, string conditions, string order = "", string grouping = "", string having = "") {
        return $"SELECT {selection} FROM {source} {conditions} {order} {grouping} {having}".Trim();
    }
    public static string InsertCommand(string target, IEnumerable<Dictionary<string, object?>> values, string output = "") {
        List<string> fieldsToInsert = values.First().Select(rowValue => rowValue.Key).ToList();
        List<string> valuesToInsert = [];
        foreach (Dictionary<string, object?> rowValue in values) {
            valuesToInsert.Add(string.Join(", ", rowValue.Select(_ => _.Value)));
        }

        return
            $"INSERT INTO {target} ({string.Join(", ", fieldsToInsert)}) " + (
                !string.IsNullOrEmpty(output)
                    ? $"OUTPUT Inserted.{output} "
                    : string.Empty
            ) + $"VALUES ({string.Join("), (", valuesToInsert)})".Trim();
    }
    public static string InsertFromCommand(string target, string source, List<Dictionary<string, object?>> values, string conditions) {
        var fieldsToInsert = values.First()
                                   .Select(rowValue => rowValue.Key)
                                   .ToList();

        var valuesToInsert = new List<string>();

        foreach (var rowValue in values) {
            valuesToInsert.Add(string.Join(", ", rowValue.Select(_ => _.Value + " as " + _.Key)));
        }

        return
            $"INSERT INTO {target} ({string.Join(", ", fieldsToInsert)}) " +
            $"SELECT {string.Join(", ", valuesToInsert)} " +
            $"FROM {source} " +
            $"{conditions}"
               .Trim();
    }

    public static string UpdateCommand(string updates, string source, string conditions) {
        return $"UPDATE {source} " +
               $"SET {updates} " +
               $"{conditions}"
                  .Trim();
    }

    public static string DeleteCommand(string source, string conditions) {
        if (string.IsNullOrEmpty(conditions))
            throw new ArgumentNullException(nameof(conditions));

        return $"DELETE FROM {source} " +
               $"{conditions}"
                  .Trim();
    }
}