import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

/**
 * @author chy
 * @date 2019-10-19 21:07
 */
public class TestRedis {
    public static void main(String[] args) {
        //连接本地的 Redis 服务
        Jedis jedis = new Jedis("hadoop102",6379);
        //查看服务是否运行，打出pong表示OK
        System.out.println("connection is OK==========>: "+jedis.ping());

    }
}
