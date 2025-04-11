package site.hnfy258.rdb.core;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.*;
import site.hnfy258.rdb.constants.RDBConstants;
import site.hnfy258.utils.SkipList;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class RDBSaver implements Saver{
    private final RedisCore redisCore;
    private final ExecutorService ioExecutor;
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(RDBSaver.class);

    public RDBSaver(RedisCore redisCore, ExecutorService ioExecutor) {
        this.redisCore = redisCore;
        this.ioExecutor = ioExecutor;
    }

    public CompletableFuture<Void> saveFullRDB() {
        return CompletableFuture.runAsync(() -> {
            File tempFile = new File(RDBConstants.RDB_FILE_NAME + ".tmp");
            
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
                RDBUtil.writeRDBHeader(dos);
                saveAllDatabases(dos);
                RDBUtil.writeRDBFooter(dos);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
            
            renameTempFile(tempFile, new File(RDBConstants.RDB_FILE_NAME));
        }, ioExecutor);
    }

    public CompletableFuture<Void> saveIncrementalRDB(Map<Integer, Map<BytesWrapper, Long>> lastModifiedMap) {
        return CompletableFuture.runAsync(() -> {
            File tempFile = new File(RDBConstants.RDB_FILE_NAME + ".inc.tmp");
            
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
                RDBUtil.writeRDBHeader(dos);
                
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                
                for (Map.Entry<Integer, Map<BytesWrapper, Long>> dbEntry : lastModifiedMap.entrySet()) {
                    int dbIndex = dbEntry.getKey();
                    Map<BytesWrapper, Long> modifiedKeys = dbEntry.getValue();
                    
                    if (!modifiedKeys.isEmpty()) {
                        RDBUtil.writeSelectDB(dos, dbIndex);
                        Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);
                        
                        for (Map.Entry<BytesWrapper, Long> entry : modifiedKeys.entrySet()) {
                            BytesWrapper key = entry.getKey();
                            RedisData value = dbData.get(key);
                            if (value != null) {
                                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                                    try {
                                        saveEntry(dos, key, value);
                                    } catch (IOException e) {
                                        throw new CompletionException(e);
                                    }
                                }, ioExecutor);
                                futures.add(future);
                            }
                        }
                    }
                }
                
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                RDBUtil.writeRDBFooter(dos);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
            
            renameTempFile(tempFile, new File(RDBConstants.RDB_FILE_NAME + ".inc"));
        }, ioExecutor);
    }

    private void saveAllDatabases(DataOutputStream dos) throws IOException {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int dbIndex = 0; dbIndex < redisCore.getDbNum(); dbIndex++) {
            Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);
            if (!dbData.isEmpty()) {
                RDBUtil.writeSelectDB(dos, dbIndex);
                
                for (Map.Entry<BytesWrapper, RedisData> entry : dbData.entrySet()) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        try {
                            saveEntry(dos, entry.getKey(), entry.getValue());
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    }, ioExecutor);
                    futures.add(future);
                }
            }
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void saveEntry(DataOutputStream dos, BytesWrapper key, RedisData value) throws IOException {
        switch (value.getClass().getSimpleName()) {
            case "RedisString":
                saveString(dos, key, (RedisString) value);
                break;
            case "RedisList":
                saveList(dos, key, (RedisList) value);
                break;
            case "RedisSet":
                saveSet(dos, key, (RedisSet) value);
                break;
            case "RedisHash":
                saveHash(dos, key, (RedisHash) value);
                break;
            case "RedisZset":
                saveZset(dos, key, (RedisZset) value);
                break;
            default:
                logger.warn("未知的数据类型: " + value.getClass().getSimpleName());
        }
    }

    private void saveString(DataOutputStream dos, BytesWrapper key, RedisString value) throws IOException {
        dos.writeByte(RDBConstants.STRING_TYPE);
        RDBUtil.writeString(dos, key.getBytes());
        RDBUtil.writeString(dos, value.getValue().getBytes());
    }

    private void saveList(DataOutputStream dos, BytesWrapper key, RedisList value) throws IOException {
        dos.writeByte(RDBConstants.LIST_TYPE);
        RDBUtil.writeString(dos, key.getBytes());
        RDBUtil.writeLength(dos, value.size());
        for (BytesWrapper element : value.getAllElements()) {
            RDBUtil.writeString(dos, element.getBytes());
        }
    }

    private void saveSet(DataOutputStream dos, BytesWrapper key, RedisSet value) throws IOException {
        dos.writeByte(RDBConstants.SET_TYPE);
        RDBUtil.writeString(dos, key.getBytes());
        RDBUtil.writeLength(dos, value.size());
        for (BytesWrapper element : value.keys()) {
            RDBUtil.writeString(dos, element.getBytes());
        }
    }

    private void saveHash(DataOutputStream dos, BytesWrapper key, RedisHash value) throws IOException {
        dos.writeByte(RDBConstants.HASH_TYPE);
        RDBUtil.writeString(dos, key.getBytes());
        Map<BytesWrapper, BytesWrapper> hashMap = value.getMap();
        RDBUtil.writeLength(dos, hashMap.size());
        for (Map.Entry<BytesWrapper, BytesWrapper> e : hashMap.entrySet()) {
            RDBUtil.writeString(dos, e.getKey().getBytes());
            RDBUtil.writeString(dos, e.getValue().getBytes());
        }
    }

    private void saveZset(DataOutputStream dos, BytesWrapper key, RedisZset value) throws IOException {
        dos.writeByte(RDBConstants.ZSET_TYPE);
        RDBUtil.writeString(dos, key.getBytes());
        int zsetSize = value.size();
        RDBUtil.writeLength(dos, zsetSize);
        List<SkipList.Node> allNodes = value.getRange(0, zsetSize - 1);
        for (SkipList.Node node : allNodes) {
            RDBUtil.writeString(dos, node.member.getBytes());
            dos.writeDouble(node.score);
        }
    }

    private void renameTempFile(File tempFile, File targetFile) {
        if (!tempFile.renameTo(targetFile)) {
            try (InputStream in = new FileInputStream(tempFile);
                 OutputStream out = new FileOutputStream(targetFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            }
            tempFile.delete();
        }
    }
}