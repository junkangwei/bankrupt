import com.bankrupt.client.config.ClientConfig;
import com.bankrupt.client.producer.DefaultProducer;
import com.bankrupt.client.producer.Producer;
import org.bankrupt.common.constant.ResultEnums;
import org.junit.Test;

import java.util.HashMap;

public class TestProducer {

    public static void main(String[] args) {
        Producer producer = new DefaultProducer(new ClientConfig());
        try {
            producer.start();
//            producer.createTopic("test",4);
            HashMap<String, String> header = new HashMap<>();
            for (int i = 0; i < 1; i++) {
                final String test = producer.send("test", "wjk123" + i);
                System.out.println("当前" + i + " :返回的结果是 :" + test);
            }
        }finally {
            producer.close();
        }
    }
}
