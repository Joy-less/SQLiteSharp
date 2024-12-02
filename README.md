# SQLiteSharp

SQLiteSharp is a powerful library to help you access a SQLite database in C#.

## Features

- Create tables from your .NET objects
- Manage your database with SQL commands and No-SQL functions
- Use synchronous and asynchronous APIs
- Encrypt your database with a password or key

## Background

This project is based on [SQLite-net](https://github.com/praeclarum/sqlite-net) by Krueger Systems Inc.

The purpose of SQLiteSharp is to provide improvements to the original library, which is outdated in many ways.

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
Connection.CreateTable<ShopItem>();

// Delete all existing items in the table
Connection.DeleteAll<ShopItem>();

// Insert items into the table
Connection.Insert(new ShopItem() {
    ItemName = "Apple",
    Count = 10,
});
Connection.Insert(new ShopItem() {
    ItemName = "Banana",
    Count = 5,
});

// Find one item in the table matching a predicate
ShopItem? Apple = Connection.Find<ShopItem>(ShopItem => ShopItem.ItemName == "Apple");
Assert.NotNull(Apple);

// Delete an item from the table
Connection.Delete(Apple);

// Find several items in the table
List<ShopItem> Bananas = Connection.Table<ShopItem>().Where(ShopItem => ShopItem.ItemName == "Banana").ToList();
Assert.Single(Bananas);
```

## Custom Type Serialization

SQLiteSharp supports serialization for a set of common types, but custom types must be registered.

Type serialization is polymorphic, so you can register `object` as a fallback for all missing types.

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

## Versioning Guide

SQLiteSharp uses versions like "1.0" and "2.4".

#### For developers:
- Increment the major version when adding new features or making breaking changes.
- Increment the minor version when fixing bugs or making small improvements.

#### For users:
- You usually want the latest major version, although it may require some changes to your project.
- You always want the latest minor version, and there should not be any issues upgrading.