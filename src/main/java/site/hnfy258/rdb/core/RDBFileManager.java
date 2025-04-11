package site.hnfy258.rdb.core;

import site.hnfy258.rdb.constants.RDBConstants;

import java.io.File;

public class RDBFileManager implements FileManager {
    private static final String FULL_RDB_FILE = RDBConstants.RDB_FILE_NAME;
    private static final String INCREMENTAL_RDB_FILE = RDBConstants.RDB_FILE_NAME + ".inc";

    public boolean fullRdbExists() {
        return new File(FULL_RDB_FILE).exists();
    }

    public boolean incrementalRdbExists() {
        return new File(INCREMENTAL_RDB_FILE).exists();
    }

    public File getFullRdbFile() {
        return new File(FULL_RDB_FILE);
    }

    public File getIncrementalRdbFile() {
        return new File(INCREMENTAL_RDB_FILE);
    }
}