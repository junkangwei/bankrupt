import com.bankrupt.client.config.ClientConfig;
import com.bankrupt.client.producer.DefaultProducer;
import com.bankrupt.client.producer.Producer;

import java.util.HashMap;

public class TestDelayProducer {

    public static void main(String[] args) {
        Producer producer = new DefaultProducer(new ClientConfig());
        try {
            producer.start();
//            producer.createTopic("test",4);
            HashMap<String, String> header = new HashMap<>();
            for (int i = 0; i < 4; i++) {
                final String test = producer.send("test", "delayMessage" + i,1);
                System.out.println("当前" + i + " :返回的结果是 :" + test);
            }
        }finally {
            producer.close();
        }
    }
}
