import com.haibao.leveldb.api.LeveldbService;
import com.haibao.leveldb.api.LeveldbServiceImpl;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * TODO
 *
 * @author minglei.chen
 * @date 7:14 PM 4/23/21
 **/
public class LevelDBServiceTest {


    LeveldbService<UUID> leveldbService = new LeveldbServiceImpl<UUID>();

    @Test
    public void putTest() {
        String key = "k-10000";
        leveldbService.set("k-10000", UUID.randomUUID());
        UUID uuid = leveldbService.get(key);
        System.out.println(uuid);
    }
}
