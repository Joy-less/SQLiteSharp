using DotNetBrightener.LinQToSqlBuilder;

namespace SQLiteSharp.Tests;

public class ReadMeTest {
    [Fact]
    public void Test1() {
        var builder = SqlBuilder.Select<ShopItem>(Item => Item).Where(Item => Item.ItemName == "Apple");
        _ = builder;

        // Open a database connection
        using SqliteConnection Connection = new("database.db");

        // Create a table for a class
        SqliteTable<ShopItem> ShopItems = Connection.GetTable<ShopItem>();

        // Delete all existing items in the table
        Connection.DeleteAll<ShopItem>();

        // Insert items into the table
        ShopItems.InsertOne(new ShopItem() {
            ItemName = "Apple",
            Count = 10,
        });
        ShopItems.InsertOne(new ShopItem() {
            ItemName = "Banana",
            Count = 5,
        });

        // Find one item in the table matching a predicate
        ShopItem? Apple = ShopItems.FindOne(ShopItem => ShopItem.ItemName == "Apple");
        Assert.NotNull(Apple);

        // Delete an item from the table
        Connection.Delete(Apple);

        // Find several items in the table
        List<ShopItem> Bananas = Connection.Table<ShopItem>().Where(ShopItem => ShopItem.ItemName == "Banana").ToList();
        Assert.Single(Bananas);
    }
    [Fact]
    public void Test2() {
        // Open a database connection
        using SqliteConnection Connection = new(":memory:");

        // Register custom type
        Connection.Orm.RegisterType<Sweet>(
            SqliteType.Text,
            serialize: (Sweet Sweet) => System.Text.Json.JsonSerializer.Serialize(Sweet),
            deserialize: (SqliteValue Value, Type ClrType) => System.Text.Json.JsonSerializer.Deserialize(Value.AsText, ClrType)
        );

        // Create a table for a class
        Connection.GetTable<SweetWrapper>();

        // Insert items into the table
        Connection.Insert(new SweetWrapper() {
            Sweet = new Sweet("orange"),
        });

        // Find one item in the table matching a predicate
        SweetWrapper? Sweet = Connection.Table<SweetWrapper>().FirstOrDefault();
        Assert.NotNull(Sweet);
    }
}

public class ShopItem {
    [PrimaryKey, AutoIncrement]
    public long Id { get; set; }

    [NotNull]
    public string? ItemName { get; set; }

    public long Count { get; set; }

    [Ignore]
    public int SomethingToIgnore { get; set; }
}

public class SweetWrapper {
    public Sweet? Sweet { get; set; }
}

public class Sweet(string Flavour) {
    public string? Flavour { get; set; } = Flavour;
}