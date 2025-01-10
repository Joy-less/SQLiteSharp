using System.Text.Json;

namespace SQLiteSharp.Tests;

public class ReadMeTest {
    [Fact]
    public void Test1() {
        // Open a database connection
        using SqliteConnection Connection = new("database.db");

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
        ShopItem? Apple = ShopItems.FindOne(ShopItem => ShopItem.ItemName == "Apple");
        Assert.NotNull(Apple);

        // Delete an item from the table
        ShopItems.DeleteByKey(Apple.Id);

        // Find several items in the table
        List<ShopItem> Bananas = ShopItems.Find(ShopItem => ShopItem.ItemName == "Banana").ToList();
        Assert.Single(Bananas);
    }
    [Fact]
    public void Test2() {
        // Open a database connection
        using SqliteConnection Connection = new(":memory:");

        // Register custom type
        Connection.Orm.RegisterType<Sweet>(
            SqliteType.Text,
            serialize: (Sweet Sweet) => JsonSerializer.Serialize(Sweet),
            deserialize: (SqliteValue Value, Type ClrType) => (Sweet?)JsonSerializer.Deserialize(Value.CastText, ClrType)
        );

        // Create a table for a class
        SqliteTable<SweetWrapper> Sweets = Connection.GetTable<SweetWrapper>();

        // Insert items into the table
        Sweets.Insert(new SweetWrapper() {
            Sweet = new Sweet("orange"),
        });

        // Find the first item in the table
        SweetWrapper? Sweet = Sweets.FindAll().FirstOrDefault();
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