import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DBConnector {
    static Connection connectDB() throws SQLException {
        String url = "jdbc:sqlite:AdValue.db";
        return DriverManager.getConnection(url);
    }
}