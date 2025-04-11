package site.hnfy258.rdb.core;

import java.io.File;
import java.io.IOException;

public interface Loader {
    void loadRDB(File file) throws IOException;
    void clearAllDatabases();

}
