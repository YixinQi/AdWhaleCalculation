import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

class DBConnector {
    static Connection connectDB() throws SQLException {
        String url = "jdbc:sqlite:AdValue.db";
        return DriverManager.getConnection(url);
    }

    private static String getTodayDate() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
        return dateFormat.format(date);
    }

    public static void main(String[] args) {
        String date = getTodayDate();
        TableManager tableManager = new TableManager(date);

        try {
            if (!tableManager.checkAllTablesExistence()) {
                tableManager.creteDBTables();
            }

            if (tableManager.checkOpsHasReaded()) {
                throw new RuntimeException("has been readed today");
            }

            tableManager.loadDailyValueCSV(date + "ads_LTV.csv", true);
            tableManager.loadNewUserCSV(date + "newUser.csv", true);
            tableManager.insertOrUpdateUserValues();
            tableManager.recordOps();
            tableManager.insertOrUpdateThreshold();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}