import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.haibao.leveldb.cache.LeveldbCache;
import java.util.concurrent.TimeUnit;

/**
 * GuavaCacheDemo
 *
 * @author ml.c
 * @date 8:33 PM 4/24/21
 **/
public class GuavaCacheDemo1 {

    public static void main(String[] args) {
        CacheLoader<String, String> loader = new CacheLoader<String, String>() {
            public String load(String key) throws Exception {
                Thread.sleep(1000);
                if ("key".equals(key)) return null;
                System.out.println(key + " is loaded from a cacheLoader!");
                return key + "'s value";
            }
        };

        RemovalListener<String, String> removalListener = new RemovalListener<String, String>() {
            public void onRemoval(RemovalNotification<String, String> removal) {
                System.out.println("[" + removal.getKey() + ":" + removal.getValue() + "] is evicted!");
            }
        };

        LoadingCache<String, String> testCache = CacheBuilder.newBuilder()
                .maximumSize(7)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .removalListener(removalListener)
                .build(loader);

//        Cache testCache = new LeveldbCache();

        for (int i = 0; i < 10; i++) {
            String key = "key" + i;
            String value = "value" + i;
            testCache.put(key, value);
            System.out.println("[" + key + ":" + value + "] is put into cache!");
        }

        System.out.println(testCache.getIfPresent("key6"));

        try {
            System.out.println(testCache.get("key"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
