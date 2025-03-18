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
        Item.NullableIntWithNullValue.ShouldBeNull();
        Item.NullableIntWithNonNullValue.ShouldBe(3);
        Item.NonNullableInt.ShouldBe(4);
    }
    [Fact]
    public void TestSqliteValue() {
        // Test nullable integer with non-null value
        SqliteValue NullableIntWithNullValue = (long?)null;
        Should.Throw<NullReferenceException>(() => NullableIntWithNullValue.CastInteger);
        NullableIntWithNullValue.AsInteger.ShouldBeNull();
        NullableIntWithNullValue.IsNull.ShouldBeTrue();

        // Test nullable integer with non-null value
        SqliteValue NullableIntWithNonNullValue = (long?)3;
        NullableIntWithNonNullValue.CastInteger.ShouldBe(3);
        NullableIntWithNonNullValue.AsInteger.ShouldBe(3);
        NullableIntWithNonNullValue.IsNull.ShouldBe(false);

        // Test non-nullable integer
        SqliteValue NonNullableInt = (long)4;
        NonNullableInt.CastInteger.ShouldBe(4);
        NonNullableInt.AsInteger.ShouldBe(4);
        NonNullableInt.IsNull.ShouldBe(false);
    }
}

public struct NullableItem {
    public int? NullableIntWithNullValue { get; set; }
    public int? NullableIntWithNonNullValue { get; set; }
    public int NonNullableInt { get; set; }
}