import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

class TableManager {
    private static final String CREATE_DB_SCRIPT = "create_advalue.sql";
    private static final String DAILY_VALUE_TABLE = "daily_value";
    private static final String DAILY_NEW_USER_TABLE = "daily_new_user";
    private static final String USER_VALUE_TABLE = "user_value";
    private static final String AD_VALUE_INSERT_SQL = "INSERT INTO daily_value (device_id, ad_unit, impression, ads_value) VALUES (?,?,?,?)";
    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS";
    private static final String CREATE_DAILY_VALUE_TABLE_SQL = "CREATE TABLE daily_value(\n" +
            "\tdevice_id CHAR(100) NOT NULL,\n" +
            "\tad_unit CHAR(100) NOT NULL,\n" +
            "\timpression INT NOT NULL,\n" +
            "\tads_value REAL NOT NULL,\n" +
            "   PRIMARY KEY (device_id, ad_unit)\n" +
            ");";
    private static final String NEW_USER_INSERT_SQL = "INSERT INTO daily_new_user (device_id) VALUES (?)";
    private static final String CREATE_NEW_USER_TABLE_SQL = "CREATE TABLE daily_new_user(\n" +
            "\tdevice_id CHAR(100) NOT NULL,\n" +
            "   PRIMARY KEY device_id\n" +
            ");";


    static void loadDailyCSV(String csvFile,
                             boolean truncateBeforeLoad) throws Exception {
        if (truncateBeforeLoad) {
            dropTable(DAILY_VALUE_TABLE);
            createTable(CREATE_DAILY_VALUE_TABLE_SQL);
        }

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        Connection connection = DBConnector.connectDB();

        while ((line = br.readLine()) != null) {
            PreparedStatement preparedStatement = connection.prepareStatement(AD_VALUE_INSERT_SQL);
            String[] values = line.split(",");
            preparedStatement.setString(1, values[0]);
            preparedStatement.setString(2, values[1]);
            preparedStatement.setInt(3, Integer.parseInt(values[2]));
            preparedStatement.setDouble(4, Double.parseDouble(values[3]));

            preparedStatement.executeUpdate();
        }

        br.close();
        connection.createStatement();
    }


    static void insertNewUser(String csvFile, boolean truncateBeforeLoad) throws Exception{

        if (truncateBeforeLoad) {
            dropTable(DAILY_NEW_USER_TABLE);
            createTable(CREATE_NEW_USER_TABLE_SQL);
        }
        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        Connection connection = DBConnector.connectDB();

        while ((line = br.readLine()) != null) {
            PreparedStatement preparedStatement = connection.prepareStatement(NEW_USER_INSERT_SQL);
            String[] values = line.split(",");
            preparedStatement.setString(1, values[0]);
            preparedStatement.executeUpdate();
        }

        br.close();
        connection.createStatement();
    }

    static void groupValueByUser(String date) throws Exception {
        // update old user value
        updateOldUser(date);
        // insert new user value
        insertNewUserValue();
    }


    static void updateOldUser(String date) throws SQLException, ClassNotFoundException, ParseException {
        //update history user value
        String sql = "select u.*, sum(v.ads_value) as revenue " +
                "from user_value u " +
                "left join daily_value v " +
                "on u.device_id = v.device_id " +
                "group by v.device_id";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        Date now = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM dd hh:mm:ss z yyyy", Locale.ENGLISH);
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
        while (rs.next()) {
            statement = connection.createStatement();
            String deviceId = rs.getString("device_id");
            String value = rs.getString("revenue");
            int in_app_age = rs.getInt("in_app_age") + 1;
            String ltvColumn = "d1_ltv";
            Date lastUpdateTime = simpleDateFormat.parse(rs.getString("last_update_time"));
            String dateStr = dateFormat.format(lastUpdateTime);
            if (dateStr.equals(date)) {//应该改为更新日期必须是大于上一次更新的日期，不能更新历史数据
                continue;
            }
            if (in_app_age > 30) {
                deleteUserAgeOver30(deviceId);
                continue;
            }
            value = value != null ? value : "0";
            switch (in_app_age) {
                case 2:
                    ltvColumn = "d2_ltv";
                    break;
                case 3:
                    ltvColumn = "d3_ltv";
                    break;
                case 4:
                    ltvColumn = "d4_ltv";
                    break;
                case 5:
                    ltvColumn = "d5_ltv";
                    break;
                case 6:
                    ltvColumn = "d6_ltv";
                    break;
                case 7:
                    ltvColumn = "d7_ltv";
                    break;
            }


            String sqlUpdateValue = "update user_value " +
                    "set " + ltvColumn + " = \"" + value + "\", " +
                    "last_update_time = \"" + now + "\", " +
                    "in_app_age = " + in_app_age + " " +
                    "where device_id = \"" + deviceId + "\"";
            statement.executeUpdate(sqlUpdateValue);
        }
    }

    static void insertNewUserValue() throws SQLException, ClassNotFoundException{
        String sql = "select n.device_id as device_id, sum(v.ads_value) as revenue " +
                "from daily_new_user n " +
                "left join daily_value v " +
                "on n.device_id = v.device_id " +
                "group by v.device_id";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        Date updateDate = new Date();

        while (rs.next()) {
            statement = connection.createStatement();
            String deviceId = rs.getString("device_id");
            if (!isNewUser(deviceId)) {
                continue;
            }
            String value = rs.getString("revenue");
            String sqlInsertValue = "insert into user_value values(\"" + deviceId + "\", " + value + ", 0, 0, 0, 0, 0, 0, 1, \"" + updateDate + "\")";
            statement.executeUpdate(sqlInsertValue);
        }
    }

    static void deleteUserAgeOver30(String deviceId) throws SQLException, ClassNotFoundException{
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();

        String flushSql = "DELETE FROM " + "'" + USER_VALUE_TABLE + "' WHERE device_id = " + "'" + deviceId+ "'";
        statement.executeUpdate(flushSql);
        statement.close();
        connection.close();
    }

    static void calculateThreshold() {

    }

    private static void dropTable(String tableName) throws SQLException, ClassNotFoundException {
        Connection connection = DBConnector.connectDB();
        Statement dropStatement = connection.createStatement();

        String dropSql = DROP_TABLE_SQL + " '" + tableName + "'";
        dropStatement.executeUpdate(dropSql);

        dropStatement.close();
        connection.close();
    }


    public static boolean checkOpsHasReaded(String date) throws SQLException, ClassNotFoundException{
        String sql = "select * from daily_operation_record where date = \"" + date + "\"";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            return true;
        }
        return false;
    }

    public static void recordOps(String date) throws SQLException, ClassNotFoundException{
        String sql = "INSERT INTO daily_operation_record VALUES (\"" + date + "\")";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    private static void flushTable(String tableName) throws Exception{
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();

        String flushSql = "DELETE FROM " + " '" + tableName + "'";
        statement.executeUpdate(flushSql);

        statement.close();
        connection.close();
    }

    static void creteDB() throws SQLException, IOException, ClassNotFoundException {
        Connection connection = DBConnector.connectDB();
        Statement createDBStatement = connection.createStatement();

        BufferedReader br = new BufferedReader(new FileReader(CREATE_DB_SCRIPT));
        String line;
        StringBuilder createSql = new StringBuilder();

        while ((line = br.readLine()) != null) {
            createSql.append(line);
        }

        createDBStatement.executeUpdate(createSql.toString());

        createDBStatement.close();
        connection.close();
    }

    private static void createTable(String createSql) throws SQLException, ClassNotFoundException{
        Connection connection = DBConnector.connectDB();
        Statement createStatement = connection.createStatement();

        createStatement.executeUpdate(createSql);

        createStatement.close();
        connection.close();
    }

    private static boolean isNewUser(String device_id)  throws SQLException, ClassNotFoundException {
        String sql  = "select * from user_value where device_id = \"" + device_id + "\"";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            return false;
        }

        return true;
    }

    private static boolean isOldUser(String device_id) {
        return true;
    }
}
