# WriteJsonDataToSQLite
Write the crawled coursera discussion forum data to SQLite 

My source data is json data crawled via hidden APIs in Coursera platform.

1. Write json data into SQLite originally;
2. Transfer from Original To Canonical database;
3. Update the information between tables(there are some intersection between table, depend on the design);
4. Crawling the forum data for the forum table;
5. Insert the forum data into the forum table.

