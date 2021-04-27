import com.google.common.cache.Cache;
import com.haibao.leveldb.cache.EmdisCacheBuilder;
import java.util.UUID;
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
                        .maximumSize(0)
                        .softValues()
                        .build();

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            emdisCache.put("test00"+i,UUID.randomUUID());
        }
        long end = System.currentTimeMillis();
        System.out.println((end-begin) / 1000);

        System.out.println(emdisCache.size());

        UUID u2 = emdisCache.getIfPresent("test004");
        System.out.println("u2="+u2);

        UUID u3 = emdisCache.getIfPresent("test005");
        System.out.println("u3="+u3);

        UUID u4 = emdisCache.getIfPresent("test00999");
        System.out.println("u4="+u4);

        emdisCache.invalidate("test004");
        UUID u5 = emdisCache.getIfPresent("test004");
        System.out.println("u5="+u5);

        System.out.println(emdisCache.size());

        Cache<String, String> stringCache2 =
                EmdisCacheBuilder.newBuilder()
                        .maximumSize(0)
                        .softValues()
                        .build();
        stringCache2.put("str001","hello world");
        System.out.println(stringCache2.getIfPresent("str001"));

        System.out.println(emdisCache.size());
        System.out.println(stringCache2.size());

        System.out.println(stringCache2.stats());
    }

}
