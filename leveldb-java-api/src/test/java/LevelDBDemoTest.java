import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * LevelDB 测试
 *
 * @author ml.c
 * @date 11:36 PM 4/22/21
 **/
public class LevelDBDemoTest {

    private static final String PATH = "data/emdisdb";
    private static final Charset CHARSET = Charset.forName("utf-8");
    private static final File FILE = new File(PATH);

    DB db  = null;
    DBFactory factory = null;

    /**
     *
     * @throws IOException
     */
    @BeforeEach
    public void Before() throws IOException {

        factory = new Iq80DBFactory();

        Logger logger = new Logger() {
            public void log(String message) {
                System.out.println(message);
            }
        };

        Options options = new Options();
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

        db  = factory.open(FILE, options);
    }

    @AfterEach
    public void After() {
        if (db != null) {
            try {
                db.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Destroying a database.
     */
    @Test
    public void destroy(){
        Options options = new Options();
        try {
            factory.destroy(FILE, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getDbStatus(){
        String stats = db.getProperty("leveldb.stats");
        System.out.println(stats);
    }

    @Test
    public void getApproximateSizes(){
        long[] sizes = db.getApproximateSizes(new Range(bytes("a"), bytes("u")), new Range(bytes("v"), bytes("z")));
        System.out.println("Size: "+sizes[0]+", "+sizes[1]);
    }

    /**
     * 选择范围进行整理
     */
    @Test
    public void compactRange(){

//        db.compactRange(null, null);
    }

    @Test
    public void putTest() {
        byte[] keyByte1 = "key-01".getBytes(CHARSET);
        byte[] keyByte2 = "key-02".getBytes(CHARSET);
        // 会写入磁盘中
//        db.put(keyByte1, "value-01-1".getBytes(CHARSET));
//        db.put(keyByte2, "value-02-2".getBytes(CHARSET));

        String value1 = new String(db.get(keyByte1), CHARSET);
        System.out.println(value1);
        String value2 = new String(db.get(keyByte2), CHARSET);
        System.out.println(value2);

    }

    @Test
    public void putBig(){
        //测试数据量
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            byte[] key = (i+"").getBytes(CHARSET);
            db.put(key,(i+"hell0 world!").getBytes(CHARSET));
        }
        long mid = System.currentTimeMillis();
        System.out.println(mid-begin);
//        iterator();
//        long mid2 = System.currentTimeMillis();
//        for (int i = 0; i < 1000000; i++) {
//            db.delete((i+"").getBytes(CHARSET));
//        }
//        long mid3 = System.currentTimeMillis();
//        System.out.println(mid3-mid2);
//        iterator();
//        long end = System.currentTimeMillis();
//        System.out.println((end-begin)/1000);
    }

    @Test
    public void readFromSnapshotTest() {
        // 读取当前快照，重启服务仍能读取，说明快照持久化至磁盘
        Snapshot snapshot = db.getSnapshot();
        // 读取操作
        ReadOptions readOptions = new ReadOptions();
        // 遍历中swap出来的数据，不应该保存在memtable中。
        readOptions.fillCache(false);
        // 默认snapshot为当前
        readOptions.snapshot(snapshot);

        try {

            // All read operations will now use the same
            // consistent view of the data.
            DBIterator iterator = db.iterator(readOptions);
            try {
                for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                    String key = asString(iterator.peekNext().getKey());
                    String value = asString(iterator.peekNext().getValue());
                    System.out.println(key+" = "+value);
                }
            } finally {
                // Make sure you close the iterator to avoid resource leaks.
                try {
                    iterator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } finally {
            // Make sure you close the snapshot to avoid resource leaks.
            try {
                readOptions.snapshot().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    @Test
    public void snapshotTest() {

        db.put("key-04".getBytes(CHARSET), "value-04".getBytes(CHARSET));
        // 只能得到getSnapshot之前put的值，之后的无法获取，即读取期间数据的变更，不会反应出来
        Snapshot snapshot = db.getSnapshot();
        db.put("key-05".getBytes(CHARSET), "value-05".getBytes(CHARSET));
        ReadOptions readOptions = new ReadOptions();
        readOptions.fillCache(false);
        readOptions.snapshot(snapshot);

        DBIterator iterator = db.iterator(readOptions);
        try {
            for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                System.out.println(key+" = "+value);
            }
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                readOptions.snapshot().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void writeOptionsTest() {
        // 线程安全 同时持久化
        WriteOptions writeOptions = new WriteOptions().sync(true);
        // 没有writeOptions时，会new一个，所以猜测这里添加了这个参数的意义就是可以设置sync和snapshot参数，建议采用这种方式
        db.put("key-06".getBytes(CHARSET), "value-06".getBytes(CHARSET), writeOptions);
    }

    @Test
    public void deleteTest() {
        // 存在会删除，之后查询不出，根据说明可能不是真删除，而是添加一个标记，待测试（大量数据之后删除，文件大小是否明显变化）
        db.delete("key-02".getBytes(CHARSET));
        // 不存在不会报错
        db.delete("key02".getBytes(CHARSET));

        Snapshot snapshot = db.getSnapshot();
        ReadOptions readOptions = new ReadOptions();
        readOptions.fillCache(false);
        readOptions.snapshot(snapshot);

        try {

            // All read operations will now use the same
            // consistent view of the data.
            DBIterator iterator = db.iterator(readOptions);
            try {
                for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                    String key = asString(iterator.peekNext().getKey());
                    String value = asString(iterator.peekNext().getValue());
                    System.out.println(key+" = "+value);
                }
            } finally {
                // Make sure you close the iterator to avoid resource leaks.
                try {
                    iterator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } finally {
            // Make sure you close the snapshot to avoid resource leaks.
            try {
                readOptions.snapshot().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void writeBatchTest() {
        // 批量保存，批量修改
        WriteBatch writeBatch = db.createWriteBatch();
        try {
            writeBatch.put("key-07".getBytes(CHARSET), "value-07".getBytes(CHARSET));
            writeBatch.put("key-08".getBytes(CHARSET), "value-08".getBytes(CHARSET));
            writeBatch.put("key-09".getBytes(CHARSET), "value-09".getBytes(CHARSET));
            // 这里也可以添加writeOptions
            db.write(writeBatch);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                writeBatch.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        iterator();
    }

    @Test
    public void writeBatchDeleteTest() {

        WriteBatch writeBatch = db.createWriteBatch();
        try {
            writeBatch.put("key-10".getBytes(CHARSET), "value-10".getBytes(CHARSET));
            writeBatch.put("key-11".getBytes(CHARSET), "value-11".getBytes(CHARSET));
            // 会将key-01的value置为""
            writeBatch.delete("key-04".getBytes(CHARSET));
            db.write(writeBatch);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            // Make sure you close the batch to avoid resource leaks.
            try {
                writeBatch.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        iterator();
    }

    /**
     * 迭代
     */
    @Test
    public void iterator(){

        DBIterator it = db.iterator();
        try {
            int sizeTemp = 0;

            while (it.hasNext()) {
//                Map.Entry<byte[], byte[]> next = it.next();
//                System.out.println("key = " + new String(next.getKey(),CHARSET)+" val = " + new String(next.getValue(),CHARSET));
                sizeTemp ++;
            }

            System.out.println(sizeTemp);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            // Make sure you close the iterator to avoid resource leaks.
            try {
                it.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        DBIterator iterator = db.iterator();
        try {
            int size = 0;
            for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
//                String key = asString(iterator.peekNext().getKey());
//                String value = asString(iterator.peekNext().getValue());
//                System.out.println(key+" = "+value);
                size++;
            }
            System.out.println(size);
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
