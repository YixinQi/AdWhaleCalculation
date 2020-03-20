import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

class TableManager {
    private static final String CREATE_DB_SCRIPT = "create_advalue.sql";
    private static final String DAILY_VALUE_TABLE = "daily_value";
    private static final String AD_VALUE_INSERT_SQL = "INSERT INTO daily_value (device_id, ad_unit, impression, ads_value) VALUES (?,?,?,?)";
    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS";
    private static final String CREATE_DAILY_VALUE_TABLE_SQL = "CREATE TABLE daily_value(\n" +
            "\tdevice_id CHAR(100) NOT NULL,\n" +
            "\tad_unit CHAR(100) NOT NULL,\n" +
            "\timpression INT NOT NULL,\n" +
            "\tads_value REAL NOT NULL,\n" +
            "   PRIMARY KEY (device_id, ad_unit)\n" +
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

    static void groupValueByUser() {

    }

    static void insertNewUser() {

    }

    static void updateOldUser() {

    }

    static void deleteUserAgeOver30() {

    }

    static void calculateThreshold() {

    }

    private static void dropTable(String tableName) throws SQLException {
        Connection connection = DBConnector.connectDB();
        Statement dropStatement = connection.createStatement();

        String dropSql = DROP_TABLE_SQL + " '" + tableName + "'";
        dropStatement.executeUpdate(dropSql);

        dropStatement.close();
        connection.close();
    }

    static void creteDB() throws SQLException, IOException {
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

    private static void createTable(String createSql) throws SQLException {
        Connection connection = DBConnector.connectDB();
        Statement createStatement = connection.createStatement();

        createStatement.executeUpdate(createSql);

        createStatement.close();
        connection.close();
    }

    private static boolean isNewUser(String device_id) {
        return true;
    }

    private static boolean isOldUser(String device_id) {
        return true;
    }
}
