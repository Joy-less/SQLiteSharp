namespace SQLiteSharp.Tests;

public class NullableTest {
    [Fact]
    public void Test1() {
        // Open a database connection
        using SqliteConnection Connection = new(":memory:");

        // Create a table for a class
        SqliteTable<NullableItem> NullableItems = Connection.GetTable<NullableItem>("NullableItems");

        // Insert items into the table
        NullableItems.Insert(new NullableItem() {
            NullableIntWithNullValue = null,
            NullableIntWithNonNullValue = 3,
            NonNullableInt = 4,
        });

        // Find one item in the table matching a predicate
        NullableItem Item = NullableItems.FindAll().First();
        Assert.Null(Item.NullableIntWithNullValue);
        Assert.Equal(3, Item.NullableIntWithNonNullValue);
        Assert.Equal(4, Item.NonNullableInt);
    }
    [Fact]
    public void TestSqliteValue() {
        // Test nullable integer with non-null value
        SqliteValue NullableIntWithNullValue = (long?)null;
        Assert.Throws<NullReferenceException>(() => NullableIntWithNullValue.CastInteger);
        Assert.Null(NullableIntWithNullValue.AsInteger);
        Assert.True(NullableIntWithNullValue.IsNull);

        // Test nullable integer with non-null value
        SqliteValue NullableIntWithNonNullValue = (long?)3;
        Assert.Equal(3, NullableIntWithNonNullValue.CastInteger);
        Assert.Equal(3, NullableIntWithNonNullValue.AsInteger);
        Assert.False(NullableIntWithNonNullValue.IsNull);

        // Test non-nullable integer
        SqliteValue NonNullableInt = (long)4;
        Assert.Equal(4, NonNullableInt.CastInteger);
        Assert.Equal(4, NonNullableInt.AsInteger);
        Assert.False(NonNullableInt.IsNull);
    }
}

public struct NullableItem {
    public int? NullableIntWithNullValue { get; set; }
    public int? NullableIntWithNonNullValue { get; set; }
    public int NonNullableInt { get; set; }
}