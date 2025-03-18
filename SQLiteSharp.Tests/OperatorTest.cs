namespace SQLiteSharp.Tests;

public class OperatorTest {
    [Fact]
    public void Test1() {
        // Open a database connection
        using SqliteConnection Connection = new(":memory:");

        // Create a table for a class
        SqliteTable<ShopItem> ShopItems = Connection.GetTable<ShopItem>("ShopItems");

        // Insert items into the table
        ShopItems.Insert(new ShopItem() {
            ItemName = "dragonfruit",
            Count = 1,
        });

        // Find one item in the table matching a predicate
        ShopItem? Item = ShopItems.FindOne(ShopItem => ShopItem.Count == (ShopItem.Count * 2 - ShopItem.Count));
        Item.ShouldNotBeNull();

        // Find one item in the table matching a predicate with method calls
        ShopItem? Item2 = ShopItems.FindOne(ShopItem => !string.IsNullOrEmpty(ShopItem.ItemName) && ShopItem.ItemName.Replace("dragon", "drag") == "dragfruit");
        Item2.ShouldNotBeNull();

        ShopItem? Item3 = ShopItems.FindOne(ShopItem => int.Parse(ShopItem.Count.ToString()).Equals(1));
    }
}