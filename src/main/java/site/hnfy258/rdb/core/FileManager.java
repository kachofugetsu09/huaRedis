package site.hnfy258.rdb.core;

import java.io.File;

public interface FileManager {
    boolean fullRdbExists();
    boolean incrementalRdbExists();
    File getFullRdbFile();
    File getIncrementalRdbFile();
}
