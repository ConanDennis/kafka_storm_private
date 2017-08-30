import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by Administrator on 2017/8/30.
 */

public class RedisClientTest {
    @Test
    public void testRedis() {
        Jedis jedisCli = new Jedis("slave2", 6379); //新建Jedis对象
        //jedisCli.select(2); //切换Redis数据库
        jedisCli.set("name", "helloJedis"); //与Redis命令行操作基本一致
        System.out.println(jedisCli.get("name"));
    }

}
