namespace SQLiteSharp.Tests;

public class ConvertTest {
    [Fact]
    public void Test1() {
        // Open a database connection
        using SqliteConnection Connection = new(":memory:");

        // Create a table for a class
        SqliteTable<GameEnemy> GameEnemies = Connection.GetTable<GameEnemy>("GameEnemies");

        // Insert items into the table
        GameEnemies.Insert(new GameEnemy() {
            Type = GameEnemyType.Zombie,
        });

        // Find one item in the table matching a predicate
        GameEnemies.FindOne(GameEnemy => GameEnemy.Type == GameEnemyType.Zombie).ShouldNotBeNull();
    }
}

public class GameEnemy {
    public GameEnemyType Type { get; set; }
}

public enum GameEnemyType {
    Slime,
    Zombie,
    Skeleton,
}