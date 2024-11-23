package bean;
import java.sql.Timestamp;
/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/22 15:33
 * description：
 */
public class Event {

    public String user;

    public String url;

    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
