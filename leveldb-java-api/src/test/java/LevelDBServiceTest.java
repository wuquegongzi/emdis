import com.haibao.leveldb.api.LeveldbService;
import com.haibao.leveldb.api.LeveldbServiceImpl;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * TODO
 *
 * @author ml.c
 * @date 7:14 PM 4/23/21
 **/
public class LevelDBServiceTest {

    LeveldbService<String,UUID> leveldbService = new LeveldbServiceImpl<String,UUID>();
    LeveldbService<String,String> leveldbService2 = new LeveldbServiceImpl<String,String>();

    @Test
    public void putTest() {
        String key = "k-10000";
        leveldbService.set(key, UUID.randomUUID());
        UUID uuid = leveldbService.get(key,UUID.class);
        Object uuid2 = leveldbService.get(key);
        System.out.println(uuid);
        System.out.println(uuid2);

        String key2 = "k-20000";
        leveldbService2.set(key2,"hello world!");
        String value2 = leveldbService2.get(key2,String.class);
        System.out.println(value2);
        Object value2_1 = leveldbService2.get(key2);
        System.out.println(value2_1);
    }
}
