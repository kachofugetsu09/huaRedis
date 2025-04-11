package site.hnfy258.rdb.core;

import site.hnfy258.datatype.BytesWrapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Saver {
    CompletableFuture<Void> saveFullRDB();
    CompletableFuture<Void> saveIncrementalRDB(Map<Integer, Map<BytesWrapper, Long>> lastModifiedMap);
}
