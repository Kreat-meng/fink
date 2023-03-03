package Function;

import bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author MengX
 * @create 2023/2/24 11:28:46
 *
 * TODO 实现SourceFunction接口的自定义source 并行度只能为1
 */


public class MySourceFunction implements SourceFunction<Event> {

    boolean isRuning = true;


    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();

        String[] users = {"huahua","sisi","songsong"};

        String[] urlTime = {"./home","./hdfs","./atguigu","./baidu"};

        while (isRuning){

            sourceContext.collect(

                    new Event(users[random.nextInt(users.length)],
                            urlTime[random.nextInt(urlTime.length)],
                            System.currentTimeMillis())
            );

            Thread.sleep(500);



        }
    }

    @Override
    public void cancel() {

        isRuning = false;

    }
}

