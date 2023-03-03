package bean;

import java.sql.Timestamp;


/**
 * @author MengX
 * @create 2023/2/23 16:47:07
 */
public class Event  {

    public String name;

    public String url;

    public Long timeStamp;

    public Event(String name, String url, Long timeStamp) {

        this.name = name;
        this.url = url;
        this.timeStamp = timeStamp;
    }

    public Event() {

    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timeStamp=" +new Timestamp(timeStamp) +
                '}';
    }
}
