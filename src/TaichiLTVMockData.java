import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TaichiLTVMockData {

    //private final int mockData = 100;//csv deviceids maximum

    private final int mockDefaultDataNum = 5000;//csv deviceids maximum

    private final int mockAdunitMaxNum = 10;//ad unit id maximum

    private final int mockExpectImpressionMaxNum = 10;//ad impression maximum

    private final int mockExpectECPMMaxNum = 1;//daily eCPM maximum

    private String customDate = "";//2020-03-12

    private int currentNum = 1;

    private String outPutPath = "./";

    private Map<String, String> csvHeader = new LinkedHashMap<>();

    private List<Map<String, String>> mockColumnDatas = new ArrayList<>();

    public static void main(String[] args) {
        new TaichiLTVMockData().generateCsv();
    }

    private void generateCsv() {
        initCsvHeader();
        fetchColumnDataForOneDay();
        File file = null;
        BufferedWriter csvFileOutputStream = null;
        try {
            file = new File(outPutPath);
            if (!file.exists()) {
                file.mkdirs();
            }
            String date = customDate.equals("") ? getTodayDate() : customDate;
            String fileName = date + "ads_LTV";
            file = new File(outPutPath + fileName + ".csv");
            if (!file.exists()) {
                file.createNewFile();
            }
            csvFileOutputStream = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(file), "UTF-8"),
                    1024);
            // Header
            setSingleRow(csvFileOutputStream, csvHeader);
            csvFileOutputStream.newLine();
            // LTV Data
            int totalDataNum = mockColumnDatas.size();
            for (Map<String, String> columnData : mockColumnDatas) {
                setSingleRow(csvFileOutputStream, columnData);
                totalDataNum--;
                if (totalDataNum > 0) {
                    csvFileOutputStream.newLine();
                }
            }
            csvFileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                csvFileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void setSingleRow(BufferedWriter csvFileOutputStream, Map<String, String> columnData) throws IOException {
        int totalColumnNum = csvHeader.size();
        int curColumnNum = 1;
        for (String value : columnData.values()
        ) {
            csvFileOutputStream.write(value);
            if (curColumnNum < totalColumnNum) {
                csvFileOutputStream.write(",");
            }
            curColumnNum++;
        }
    }

    private void initCsvHeader() {
        csvHeader.put("1", "DeviceId");
        csvHeader.put("2", "AdUnitId");
        csvHeader.put("3", "Impression");
        csvHeader.put("4", "Revenue");
    }

    private void fetchColumnDataForOneDay() {
        fetchColumnDataForOneDay(mockDefaultDataNum);
    }

    private void fetchColumnDataForOneDay(int needCount) {
        while (needCount > 0) {
            Map<String, String> tmpLtvColumnData = fetchColumnData();
            mockColumnDatas.add(tmpLtvColumnData);
            needCount--;
        }
        Collections.shuffle(mockColumnDatas);
    }

    private Map<String, String> fetchColumnData() {
        Map<String, String> ltvColumnData = new LinkedHashMap<>();
        //String date = customDate.equals("") ? getTodayDate() : customDate;
        ltvColumnData.put("deviceId", fetchDeviceId());
        ltvColumnData.put("adUnitId", fetchAdUnitId());
        ltvColumnData.put("impression", randomImpression());
        ltvColumnData.put("Revenue", randomECPM());
        return ltvColumnData;
    }

    private String getTodayDate() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
        return dateFormat.format(date);
    }

    //prefixes device + 1 ... 50k 5000000
    private String fetchDeviceId() {
        String prefixes = "device";
        Random random = new Random();
        int bound = mockDefaultDataNum > 0 ? mockDefaultDataNum : 50000;
        String deviceId = prefixes + String.format("%07d", currentNum);
        currentNum++;
        return deviceId;
    }

    //prefixes adunit + random 1 2 3 .... 10
    private String fetchAdUnitId() {
        String prefixes = "adunit";
        Random random = new Random();
        int bound = mockAdunitMaxNum > 0 ? mockAdunitMaxNum : 10;
        int randomNum = random.nextInt(bound) + 1;
        String adUnitId = prefixes + String.format("%02d", randomNum);
        return adUnitId;
    }

    // < 10
    private String randomImpression() {
        Random random = new Random();
        return String.valueOf(random.nextInt(mockExpectImpressionMaxNum + 1));//Maybe the impression is 0
    }

    // < 1
    private String randomECPM() {
        double randomECPM = Math.random();
        if (mockExpectECPMMaxNum > 1) {
            Random random = new Random();
            int randomPrefixes = random.nextInt(mockExpectECPMMaxNum);
            if (randomPrefixes >= 1) {
                randomECPM += randomPrefixes;
            }
        }
        DecimalFormat df = new DecimalFormat("#0.00");
        return df.format(randomECPM);
    }
}
