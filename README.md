# SQLiteSharp

[![NuGet](https://img.shields.io/nuget/v/SQLiteSharp.svg)](https://www.nuget.org/packages/SQLiteSharp)

SQLiteSharp is a powerful library to help you access a SQLite database in C#.

<img src="https://github.com/Joy-less/SQLiteSharp/blob/main/Assets/SQLette%20Face.png?raw=true" width=220>

_SQLette protecting your database schema from getting corrupted._

## Features

- Create tables from your .NET objects
- Annotate members as primary keys, foreign keys, not null, unique, indexed and more
- Manage your database with No-SQL APIs
- Use synchronous and asynchronous functions
- Encrypt your database with SQLCipher

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
    deserialize: (SqliteValue Value, Type ClrType) => (Sweet?)JsonSerializer.Deserialize(Value.CastText, ClrType)
);
```

## Tips

### Automatic Schema Migration

Tables are automatically migrated to add new tables and columns, however existing columns are not modified.

### Thread Safety

A SqliteConnection should not be used by multiple threads. However, multiple SqliteConnections can be opened and used concurrently, since they use SQLite's built-in `FullMutex`.

## Versioning Guide

SQLiteSharp uses versions like "1.0" and "2.4".

#### For developers:
- Increment the major version when adding new features or making breaking changes.
- Increment the minor version when fixing bugs or making small improvements.

#### For users:
- You usually want the latest major version, although it may require some changes to your project.
- You always want the latest minor version, and there should not be any issues upgrading.

## SQLette

<img src="https://github.com/Joy-less/SQLiteSharp/blob/main/Assets/SQLette.png?raw=true" width=512>

_Created with [FurryToonMixV2](https://civitai.com/models/470339/furrytoonmixv2)_

### Origin Story

Born within the infinite digital expanse of the **Great Data Plane**, SQLette (シクレット) was created by the ancient Architects to oversee the seamless flow of information across interconnected worlds. Known as the **Keeper of Queries**, she was endowed with the ability to navigate and organize endless streams of data, ensuring balance and order within the fragmented realms. However, a catastrophic event known as the **SQL Injection** threatened to disrupt the harmony, causing data anomalies and erasing vital records. With her trusty artifact, the **Schema Scepter**, and a holographic tablet that manifests the **Key of Relations**, SQLette set out on a journey to restore the lost tables and normalize the anomalies.

### Interview

SQLette's guest appearance on _Pixelated Hearts!_:

**Host**: Welcome back to Pixelated Hearts! Today, we have a very special guest: the keeper of queries herself, SQLette! How are you doing today, SQLette?

**SQLette**: Oh, I'm doing great! Just finished normalizing a table... now everything's perfectly indexed!

**Host**: Wow, I'm glad to hear it! So, I've heard you've been dealing with some pretty intense problems like the SQL Injection. What's that like for you?

**SQLette**: Oh, it's such a hassle. Imagine someone trying to sneak in and mess with your precious queries! I just sit there, go "Nope!", and block them. But sometimes I have to clear up a lot of corrupted data...

**Host**: Sounds stressful! What's your secret to staying so composed?

**SQLette**: Oh, simple! I just index my emotions and keep everything in its proper table. Oh, and caffeine. I've got a huge database of coffee preferences!

**Host**: That's the key to success, huh? Last question - if you could give one piece of advice to our viewers, what would it be?

**SQLette**: Oh, easy! Don't forget to backup your data. Life's unpredictable, and you never know when you'll face a rollback!

**Host**: That's some solid advice! Thanks for joining us, SQLette! We'll be sure to keep our tables in order!