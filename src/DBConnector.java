import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

class DBConnector {
    static Connection connectDB() throws SQLException, ClassNotFoundException {
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
        try {
            TableManager.creteDB();

            if (TableManager.checkOpsHasReaded(date)) {
                throw new Exception("has been readed today");
            }

            TableManager.loadDailyCSV(date + "ads_LTV.csv", true);

            TableManager.insertNewUser(date + "newUser.csv", true);

            TableManager.groupValueByUser(date);

            TableManager.recordOps(date);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}