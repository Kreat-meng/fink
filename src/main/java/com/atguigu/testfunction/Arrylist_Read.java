package com.atguigu.testfunction;

import bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author MengX
 * @create 2023/2/23 16:50:16
 */
public class Arrylist_Read {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Event> eventList = Arrays.asList(new Event("songsong", "wwww.atguigu.com", 1000l));

        /*ArrayList<Event> events = new ArrayList<>();
        Event tongtong = new Event("tongtong", "www.baidu.com", 2000l);

        events.add(tongtong);

        System.out.println(events);*/

        DataStreamSource<Event> collectionSource
                = env.fromCollection(eventList);

        collectionSource.print();

        env.execute();

    }
}
