# AdWhaleCalculation
The main method in Executor class needs to be run once and only once a day. Any malformed data will be discarded.

The input CSV file should be named as YYYY-MM-DDads_LTV.csv. The file should contain four columns, device id, ad unit id, impression, revenue, seperated by "," and no header. There is a sample file included. Currently the code picks up the file from the relative path. To change it to an absolute path, please modify line 25 in Executor.java. 

This program uses SQLite as a local database. It will be created automatically. Currently, the program is hard coded to preserve data for 30 days. To change this, you need to modify line 32 in TableManager.java.

The threshold for each day is history data's average. To change it to a customized percentage, you need to rebuild the sql query in line 365 TableManager.java.

To limit signals to AC, we only output high value users on D2, D4 and D6. To configure this, you could modify line 136-138 in TableManager.java.
