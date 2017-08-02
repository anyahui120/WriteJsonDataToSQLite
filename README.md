# WriteJsonDataToSQLite
Write the crawled coursera discussion forum data to SQLite 

My source data(saved in '/data/') is json data crawled via hidden APIs in Coursera platform.

STEP1: Write crawled raw data(saved in json files) into SQLite tables: user, thread and post.(writeToSQLite.py)

    python writeToSQLite.py

STEP2: Write raw data into canonical tables(design scheme from <a href= "https://github.com/cmkumar87">Muthu</a>).

    python fromOriginalToCanonical.py

STEP3: Some supplement for each canonical table.

    python transferFromTables.py

Please set your PATH of your database and your json data files in <a href = "https://github.com/anyahui120/WriteJsonDataToSQLite/blob/master/config.yml">config.yml</a>. 

    1. Please save all the data in one directory '/data/' or create a directory you prefer(remember to set it in the config.yml).
    
    2. Please create a new database and set the path in config.yml.

