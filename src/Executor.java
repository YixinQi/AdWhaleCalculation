import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Executor {
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

            if (tableManager.checkOpsHasRead()) {
                throw new RuntimeException("has been readed today");
            }

            String dailyValueCSVFile = date + "ads_LTV.csv";
            File file = new File(dailyValueCSVFile);
            if (!file.exists()) {
                throw new RuntimeException("no ads_LTV CSV file today");
            }

            String dailyNewUserCSVFile = date + "newUser.csv";
            file = new File(dailyNewUserCSVFile);
            if (!file.exists()) {
                throw new RuntimeException("no newUser CSV file today");
            }
            
            System.out.println("Start Time" + new Date());
            tableManager.loadDailyValueCSV(dailyValueCSVFile, true);
            tableManager.loadNewUserCSV(dailyNewUserCSVFile, true);
            System.out.println("update user value: " + new Date());
            tableManager.insertOrUpdateUserValues();
            tableManager.recordOps();
            tableManager.insertOrUpdateThreshold();
            tableManager.generateAdWhaleDeviceIds();
            System.out.println("End Time" + new Date());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
