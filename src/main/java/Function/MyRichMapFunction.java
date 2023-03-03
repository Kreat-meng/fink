package Function;

import bean.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author MengX
 * @create 2023/2/24 19:26:28
 */
public class MyRichMapFunction extends RichMapFunction<Event,Tuple3<String,String,String>> {


    @Override
    public void open(Configuration parameters) throws Exception {

        System.out.println("输出类似这样的字段:" + "songsong"+"./home"+"2020-08-12,这样的Tuple3 元组");

        Tuple3<Event, String, Date> mapTuple3 = new Tuple3<>();

        super.open(parameters);


    }


    @Override
    public void close() throws Exception {

        System.out.println("要关闭了！");
        super.close();
    }


    @Override
    public Tuple3<String,String,String> map(Event event) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date1 = new Date(event.timeStamp);

        Tuple3<String, String, String> result = Tuple3.of(event.name, event.url, simpleDateFormat.format(date1));

        return result;
    }
}
