import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DBConnector {
    static Connection connectDB() throws SQLException {
        String url = "jdbc:sqlite:AdValue.db";
        return DriverManager.getConnection(url);
    }

    public static void main(String[] args) {
        try {
            TableManager.creteDB();
            TableManager.loadDailyCSV("2020-03-12ads_LTV.csv", false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}