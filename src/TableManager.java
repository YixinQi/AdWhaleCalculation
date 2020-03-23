import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class TableManager {
    private static final String CREATE_DB_SCRIPT = "create_advalue.sql";
    private static final String CREATE_DAILY_VALUE_TABLE_SCRIPT = "create_daily_value_table.sql";
    private static final String CREATE_NEW_USER_TABLE_SCRIPT = "create_new_user_table.sql";
    private static final String CREATE_VALUE_THRESHOLD_SCRIPT = "create_value_threshold_table.sql";
    private static final String DAILY_VALUE_TABLE = "daily_value";
    private static final String DAILY_NEW_USER_TABLE = "daily_new_user";
    private static final String USER_VALUE_TABLE = "user_value";
    private static final String VALUE_THRESHOLD_TABLE = "value_threshold";
    private static final String DAILY_OPERATION_TABLE = "daily_operation_record";
    private static final String AD_VALUE_INSERT_SQL = "INSERT INTO daily_value (device_id, ad_unit, impression, ads_value) VALUES (?,?,?,?)";
    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS";
    private static final String NEW_USER_INSERT_SQL = "INSERT INTO daily_new_user (device_id) VALUES (?)";
    private static final String THRESHOLD_INSERT_SQL = "INSERT INTO value_threshold VALUES (?,?,?,?,?,?,?)";
    private static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat("EEE MMM dd hh:mm:ss z yyyy", Locale.ENGLISH);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("YYYY-MM-dd");
    private static final String outPutPath = "./";
    private static final String AD_WHALE_FILE = "AdWhaleDeviceids.csv";

    private final String curDate;

    TableManager(String curDate) {
        this.curDate = curDate;
    }

    void loadDailyValueCSV(String csvFile,
                           boolean truncateBeforeLoad) throws Exception {
        if (truncateBeforeLoad) {
            dropTable(DAILY_VALUE_TABLE);
            createTable(CREATE_DAILY_VALUE_TABLE_SCRIPT);
        }

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        Connection connection = DBConnector.connectDB();
        int lineNo = 1;
        while ((line = br.readLine()) != null) {
            if (lineNo == 1) {
                lineNo++;
                continue;
            }
            PreparedStatement preparedStatement = connection.prepareStatement(AD_VALUE_INSERT_SQL);
            String[] values = line.split(",");
            preparedStatement.setString(1, values[0]);
            preparedStatement.setString(2, values[1]);
            preparedStatement.setInt(3, Integer.parseInt(values[2]));
            preparedStatement.setDouble(4, Double.parseDouble(values[3]));

            preparedStatement.executeUpdate();
            lineNo++;
            preparedStatement.close();
        }

        br.close();
        connection.close();
    }

    void loadNewUserCSV(String csvFile, boolean truncateBeforeLoad) throws Exception {
        if (truncateBeforeLoad) {
            dropTable(DAILY_NEW_USER_TABLE);
            createTable(CREATE_NEW_USER_TABLE_SCRIPT);
        }

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        Connection connection = DBConnector.connectDB();

        int lineNo = 1;
        while ((line = br.readLine()) != null) {
            if (lineNo == 1) {
                lineNo++;
                continue;
            }
            PreparedStatement preparedStatement = connection.prepareStatement(NEW_USER_INSERT_SQL);
            String[] values = line.split(",");
            preparedStatement.setString(1, values[0]);
            preparedStatement.executeUpdate();
        }

        br.close();
        connection.close();
    }

    void insertOrUpdateUserValues() throws Exception {
        updateOldUserValues();
        insertNewUserValues();
    }

    void insertOrUpdateThreshold() throws SQLException, IOException {
        dropTable(VALUE_THRESHOLD_TABLE);
        createTable(CREATE_VALUE_THRESHOLD_SCRIPT);

        Connection connection = DBConnector.connectDB();
        PreparedStatement preparedStatement = connection.prepareStatement(THRESHOLD_INSERT_SQL);
        Map<Integer, String> avgUserValues = avgUserValue();
        for (int dayValue = 1; dayValue < 8; dayValue++) {
            preparedStatement.setString(dayValue, avgUserValues.get(dayValue));
        }
        preparedStatement.executeUpdate();

        preparedStatement.close();
        connection.close();
    }

    void generateAdWhaleDeviceIds() throws SQLException, IOException {
        String sql = "select device_id from (" +
                "select device_id,d2_ltv,d4_ltv,d6_ltv,d2_threshold,d4_threshold,d6_threshold,in_app_age from user_value " +
                "left join value_threshold " +
                ") " +
                " where  " +
                "(d2_ltv >= d2_threshold and d2_threshold > 0 and in_app_age = 2) or " +
                "(d4_ltv >= d4_threshold and d4_threshold > 0 and in_app_age = 4) or " +
                "(d6_ltv >= d6_threshold and d6_threshold > 0 and in_app_age = 6)";
                        "select device_id,d2_ltv,d4_ltv,d6_ltv,d2_threshold,d4_threshold,d6_threshold from " + USER_VALUE_TABLE + " " +
                        "left join  " + VALUE_THRESHOLD_TABLE +
                        ") " +
                    " where  " +
                    "(d2_ltv >= d2_threshold and d2_threshold > 0) or " +
                    "(d4_ltv >= d4_threshold and d4_threshold > 0) or " +
                    "(d6_ltv >= d6_threshold and d6_threshold > 0)";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);

        String fileName = curDate + AD_WHALE_FILE;
        File file = new File(outPutPath + fileName);
        if (!file.exists()) {
            file.createNewFile();
        }

        BufferedWriter csvFileOutputStream = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8),
                1024);
        while (rs.next()) {
            String deviceId = rs.getString(1);
            csvFileOutputStream.write(deviceId);
            csvFileOutputStream.newLine();
        }

        statement.close();
        connection.close();
        csvFileOutputStream.flush();
        csvFileOutputStream.close();
    }

    boolean checkOpsHasRead() throws SQLException {
        String checkCurDateOpsSql = "select * from daily_operation_record where op_date = \"" + curDate + "\"";
    boolean checkOpsHasReaded() throws SQLException {
        String checkCurDateOpsSql = "select * from " + DAILY_OPERATION_TABLE + " where op_date = \"" + curDate + "\"";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(checkCurDateOpsSql);

        while (rs.next()) {
            statement.close();
            connection.close();
            return true;
        }

        statement.close();
        connection.close();
        return false;
    }

    void recordOps() throws SQLException {
        String insertOpsDateSql = "INSERT INTO " + DAILY_OPERATION_TABLE + "  VALUES (\"" + curDate + "\")";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        statement.executeUpdate(insertOpsDateSql);

        statement.close();
        connection.close();
    }

    void creteDBTables() throws SQLException, IOException {
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

    boolean checkAllTablesExistence() throws SQLException {
        String sql = "SELECT count(*) FROM sqlite_master WHERE type='table'";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);

        while (rs.next()) {
            if (5 == rs.getInt(1)) {
                statement.close();
                connection.close();

                return true;
            } else if (0 == rs.getInt(1)) {
                statement.close();
                connection.close();

                return false;
            }
        }

        statement.close();
        connection.close();

        throw new RuntimeException("Some tables are missing.");
    }

    private void updateOldUserValues() throws SQLException, ParseException {
        String sql = "select u.*, sum(v.ads_value) as revenue " +
                "from " + USER_VALUE_TABLE + " u " +
                "left join " + DAILY_VALUE_TABLE + " v " +
                "on u.device_id = v.device_id " +
                "group by v.device_id";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        Date now = new Date();
        while (rs.next()) {
            statement = connection.createStatement();
            String deviceId = rs.getString("device_id");
            String value = rs.getString("revenue");
            int in_app_age = rs.getInt("in_app_age") + 1;
            String ltvColumn;
            Date lastUpdateTime = SQL_DATE_FORMAT.parse(rs.getString("last_update_time"));

            if (DATE_FORMAT.format(lastUpdateTime).equals(curDate)) {
                continue;
            }

            if (in_app_age > 30) {
                deleteUserAgeOver30(deviceId);
                continue;
            }

            value = value != null ? value : "0";
            ltvColumn = "d" + in_app_age + "_ltv";

            String sqlUpdateValue = "update " + USER_VALUE_TABLE;
            sqlUpdateValue += in_app_age >= 8 ? " set " : " set " + ltvColumn + " = \"" + value + "\", ";
            sqlUpdateValue += "last_update_time = \"" + now + "\", " +
                    "in_app_age = " + in_app_age + " " +
                    "where device_id = \"" + deviceId + "\"";
            statement.executeUpdate(sqlUpdateValue);

            statement.close();
            connection.close();
        }
    }

    private void insertNewUserValues() throws SQLException {
        String sql = "select n.device_id as device_id, sum(v.ads_value) as revenue " +
                "from " + DAILY_NEW_USER_TABLE + " n " +
                "left join " + DAILY_VALUE_TABLE + " v " +
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
            String insertValueSql = "insert into " + USER_VALUE_TABLE + " values(\"" + deviceId + "\", " + value + ", 0, 0, 0, 0, 0, 0, 1, \"" + updateDate + "\")";
            statement.executeUpdate(insertValueSql);
        }

        statement.close();
        connection.close();
    }

    private void deleteUserAgeOver30(String deviceId) throws SQLException {
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();

        String flushSql = "DELETE FROM " + "'" + USER_VALUE_TABLE + "' WHERE device_id = " + "'" + deviceId + "'";
        statement.executeUpdate(flushSql);

        statement.close();
        connection.close();
    }

    private void createTable(String createScript) throws SQLException, IOException {
        Connection connection = DBConnector.connectDB();
        Statement createStatement = connection.createStatement();

        BufferedReader br = new BufferedReader(new FileReader(createScript));
        String line;
        StringBuilder createSql = new StringBuilder();

        while ((line = br.readLine()) != null) {
            createSql.append(line);
        }

        createStatement.executeUpdate(createSql.toString());

        createStatement.close();
        connection.close();
    }

    private void dropTable(String tableName) throws SQLException {
        Connection connection = DBConnector.connectDB();
        Statement dropStatement = connection.createStatement();

        String dropSql = DROP_TABLE_SQL + " '" + tableName + "'";
        dropStatement.executeUpdate(dropSql);

        dropStatement.close();
        connection.close();
    }

    private boolean isNewUser(String device_id) throws SQLException {
        String checkUserSql = "select * from " + USER_VALUE_TABLE + " where device_id = \"" + device_id + "\"";
        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(checkUserSql);

        while (rs.next()) {
            statement.close();
            connection.close();
            return false;
        }

        statement.close();
        connection.close();
        return true;
    }

    private Map<Integer, String> avgUserValue() throws SQLException {
        Map<Integer, String> avgUserValues = new HashMap<>();

        StringBuilder valueSql = new StringBuilder("SELECT ");
        for (int dayValue = 1; dayValue < 8; dayValue++) {
            valueSql.append("avg(d").append(dayValue).append("_ltv)");
            valueSql.append(dayValue < 7 ? "," : "");
        }
        valueSql.append(" FROM user_value");

        Connection connection = DBConnector.connectDB();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(valueSql.toString());

        while (rs.next()) {
            for (int dayValue = 1; dayValue < 8; dayValue++) {
                avgUserValues.put(dayValue, rs.getString(dayValue));
            }
        }

        statement.close();
        connection.close();
        return avgUserValues;
    }
}
