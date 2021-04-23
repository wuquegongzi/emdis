package com.haibao.leveldb.api;

import cn.hutool.core.lang.Console;
import cn.hutool.core.util.StrUtil;
import com.haibao.leveldb.utils.ObjectAndByte;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * Leveldb java 客户端
 *
 * @author ml.c
 * @date 5:09 PM 4/23/21
 **/
public class LeveldbServiceImpl<T> implements LeveldbService<T>{

    private static final String PATH = "data/emdisdb";
//    private static final Charset CHARSET = Charset.forName("utf-8");
    private static final File FILE = new File(PATH);

    private static  DBFactory factory;
    private static  Options options;
    static {
        factory = new Iq80DBFactory();

        Logger logger = new Logger() {
            @Override
            public void log(String message) {
                Console.log(message);
            }
        };

        options = new Options();
        // 默认如果没有则创建
        options.createIfMissing(true);

        //LevelDB 的磁盘数据是以数据库块的形式存储的，默认的块大小是 4k。
        // 适当提升块大小将有益于批量大规模遍历操作的效率，如果随机读比较频繁，这时候块小点性能又会稍好，这就要求我们自己去折中选择。
        options.blockSize(8092);

        //Getting informational log messages.
        options.logger(logger);

//        //Disabling Compression
//        options.compressionType(CompressionType.NONE);

//        //默认是开启压缩
//        options.compressionType(CompressionType.SNAPPY);

        //Configuring the Cache
        // 100MB cache
        options.cacheSize(100 * 1048576);
    }

    /**
     * 获取DB
     * @return
     * @throws IOException
     */
    private DB getDB(){
        DB db  = null;
        try {
            db = factory.open(FILE, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return db;
    }

    /**
     * 关闭 DB
     */
    private void close(DB db) {
        if(null != db){
            try {
                db.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 校验key
     * @param k
     */
    private void checkNull(String k){
        if(StrUtil.isEmpty(k)){
           throw new IllegalArgumentException("key is null,please check it!");
        }
    }

    @Override
    public void set(String k, T v) {
        DB db = null;
        try {
            db = getDB();
            checkNull(k);
            db.put(bytes(k), ObjectAndByte.toByteArray(v));
        }finally {
            close(db);
        }
    }

    @Override
    public void setSync(String k, T v) {
        DB db = null;
        try {
            checkNull(k);
            db =getDB();
            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.put(bytes(k), ObjectAndByte.toByteArray(v), writeOptions);
        } finally {
            close(db);
        }
    }

    @Override
    public void setBatch(Map<String,T> map) {
        DB db = null;
        WriteBatch writeBatch = null;
        try {
            db = getDB();
            writeBatch = db.createWriteBatch();

            for (String key : map.keySet()) {
                writeBatch.put(bytes(key),ObjectAndByte.toByteArray(map.get(key)));
            }
            db.write(writeBatch);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(null != writeBatch){
                    writeBatch.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            close(db);
        }
    }

    @Override
    public T get(String k) {

        DB db = null;
        T obj = null;
        try {
            checkNull(k);
            db = getDB();
            obj = (T)ObjectAndByte.toObject(db.get(bytes(k)));
        } finally {
            close(db);
        }

        return obj;
    }

    @Override
    public boolean remove(String k) {
        DB db = null;
        try {
            checkNull(k);
            db = getDB();
            db.delete(bytes(k));
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }finally {
            close(db);
        }
        return true;
    }

    @Override
    public boolean removeSync(String k) {

        DB db = null;
        try {
            checkNull(k);
            db =getDB();
            WriteOptions writeOptions = new WriteOptions().sync(true);
            db.delete(bytes(k),writeOptions);
        } finally {
            close(db);
        }

        return false;
    }

    @Override
    public boolean removeBatch(Set<String> set) {

        DB db = null;
        WriteBatch writeBatch = null;
        try {
            db = getDB();
            writeBatch = db.createWriteBatch();
            for (String k : set) {
                writeBatch.delete(bytes(k));
            }
            db.write(writeBatch);

        }catch (Exception e){
            e.printStackTrace();
            return false;
        }finally {
            try {
                if(null != writeBatch){
                    writeBatch.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            close(db);
        }
        return true;
    }


    @Override
    public LinkedHashMap scan(String startKey, String endKey, int limit) {
        //todo
        return null;
    }

    @Override
    public Snapshot getSnapshot() {
        //todo
        return null;
    }

    @Override
    public boolean repairDB(String dbDir, Options options) {
        //todo
        return false;
    }

}
