import com.google.common.cache.Cache;
import com.haibao.leveldb.cache.EmdisCacheBuilder;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * EmdisCache test
 *
 * @author ml.c
 * @date 4:22 PM 4/25/21
 **/
public class EmdisCacheTest {

    @Test
    public void LeveldbGuavaCache() {

        Cache<String, UUID> emdisCache =
                EmdisCacheBuilder.newBuilder()
                        .expireAfterAccess(10000, TimeUnit.SECONDS)
                        .softValues()
                        .build();

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            emdisCache.put("test00"+i,UUID.randomUUID());
        }
        long end = System.currentTimeMillis();
        System.out.println((end-begin) / 1000);

        System.out.println(emdisCache.size());

        UUID u0 = emdisCache.getIfPresent("test000");
        System.out.println("u0="+u0);
        UUID u1 = emdisCache.getIfPresent("test001");
        System.out.println("u1="+u1);
        UUID u2 = emdisCache.getIfPresent("test002");
        System.out.println("u2="+u2);
        UUID u3 = emdisCache.getIfPresent("test003");
        System.out.println("u3="+u3);

        UUID u999 = emdisCache.getIfPresent("test00999");
        System.out.println("u999="+u999);

        emdisCache.invalidate("test004");
        emdisCache.invalidate("test005");
        emdisCache.invalidate("test006");
        UUID u4 = emdisCache.getIfPresent("test004");
        System.out.println("u4="+u4);

        System.out.println("emdisCache-size:"+emdisCache.size());

        Cache<String, String> stringCache =
                EmdisCacheBuilder.newBuilder()
                        .expireAfterAccess(20000, TimeUnit.SECONDS)
                        .softValues()
                        .build();
        for (int i = 0; i < 1000; i++) {
            stringCache.put("str00"+i,UUID.randomUUID().toString());
        }

        System.out.println(stringCache.getIfPresent("str001"));
        System.out.println("stringCache-size:"+stringCache.size());

        Cache<String, Long> longCache =
                EmdisCacheBuilder.newBuilder()
                        .expireAfterAccess(10000, TimeUnit.SECONDS)
                        .softValues()
                        .build();

        for (int i = 0; i < 1000; i++) {
            longCache.put("long00"+i,new SecureRandom().nextLong());
        }
        System.out.println(longCache.getIfPresent("long001"));

        System.out.println("longCache-size:"+longCache.size());

        while (true){
            System.out.println(emdisCache.size()+":"+longCache.size()+"\n");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
