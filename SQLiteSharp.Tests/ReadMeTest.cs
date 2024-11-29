namespace SQLiteSharp.Tests;

public class ReadMeTest {
    [Fact]
    public void Test1() {
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