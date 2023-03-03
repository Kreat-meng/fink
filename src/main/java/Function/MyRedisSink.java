package Function;

import bean.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @author MengX
 * @create 2023/2/27 11:47:54
 */
public class MyRedisSink<Event> extends RichSinkFunction<bean.Event> {

    public Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {

        jedis = new Jedis("hadoop102",6379);
    }

    @Override
    public void invoke(bean.Event value, Context context) throws Exception {

       jedis.set(value.name,value.toString());
    }


    @Override
    public void close() throws Exception {

        jedis.close();

    }
}
