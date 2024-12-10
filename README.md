# SQLiteSharp

SQLiteSharp is a powerful library to help you access a SQLite database in C#.

## Features

- Create tables from your .NET objects
- Manage your database with SQL commands and No-SQL functions
- Use synchronous and asynchronous APIs
- Encrypt your database with a password or key

## Background

This project was originally based on [SQLite-net](https://github.com/praeclarum/sqlite-net) by Krueger Systems Inc.

SQLiteSharp is a complete rewrite of the original, providing a modern experience akin to [MongoDB](https://www.mongodb.com) or [LiteDB](https://github.com/litedb-org/LiteDB) with the power of SQLite.

## Example

First, declare your object with optional annotations:
```cs
public class ShopItem {
    [PrimaryKey, AutoIncrement]
    public long Id { get; set; }

    [NotNull]
    public string? ItemName { get; set; }

    public long Count { get; set; }

    [Ignore]
    public int SomethingToIgnore { get; set; }
}
```

Second, open a connection to your database:
```cs
// Open a database connection
using SQLiteConnection Connection = new("database.db");

// Create a table for a class
SqliteTable<ShopItem> ShopItems = Connection.GetTable<ShopItem>();

// Delete all existing items in the table
ShopItems.DeleteAll();

// Insert items into the table
ShopItems.Insert(new ShopItem() {
    ItemName = "Apple",
    Count = 10,
});
ShopItems.Insert(new ShopItem() {
    ItemName = "Banana",
    Count = 5,
});

// Find one item in the table matching a predicate
ShopItem? Apple = Connection.Find<ShopItem>(ShopItem => ShopItem.ItemName == "Apple");

// Delete an item from the table
ShopItems.DeleteByKey(Apple.Id);

// Find several items in the table
List<ShopItem> Bananas = ShopItems.Find(ShopItem => ShopItem.ItemName == "Banana").ToList();
```

## Custom Type Serialization

SQLiteSharp supports serialization for a set of common types.
Polymorphism is supported, so you can register `object` as a fallback for all missing types.

By default, unregistered types are serialized as JSON using `System.Text.Json`.

```cs
public class SweetWrapper {
    public Sweet? Sweet { get; set; } // custom type
}
public class Sweet(string Flavour) {
    public string? Flavour { get; set; } = Flavour;
}
```

```cs
// Open a database connection
using SQLiteConnection Connection = new(":memory:");

// Register custom type
Connection.Orm.RegisterType<Sweet>(
    SqliteType.Text,
    serialize: (Sweet Sweet) => JsonSerializer.Serialize(Sweet),
    deserialize: (SqliteValue Value, Type ClrType) => JsonSerializer.Deserialize(Value.AsText, ClrType)
);
```

## Notes

- Tables are automatically migrated to add new tables and columns, however changed columns are not updated.
- A SqliteConnection should not be used by multiple threads. However, multiple SqliteConnections can be opened and used concurrently, since they use SQLite's built-in `FullMutex`.

## Versioning Guide

SQLiteSharp uses versions like "1.0" and "2.4".

#### For developers:
- Increment the major version when adding new features or making breaking changes.
- Increment the minor version when fixing bugs or making small improvements.

#### For users:
- You usually want the latest major version, although it may require some changes to your project.
- You always want the latest minor version, and there should not be any issues upgrading.