import com.bankrupt.client.config.ClientConfig;
import com.bankrupt.client.consumer.Consumer;
import com.bankrupt.client.consumer.MessageListener;
import com.bankrupt.client.consumer.PullDefaultConsumer;
import com.bankrupt.client.producer.DefaultProducer;
import com.bankrupt.client.producer.Producer;
import message.Message;

import java.nio.charset.Charset;
import java.util.HashMap;

public class TestConsumer {

    public static void main(String[] args) {
        Consumer consumer = new PullDefaultConsumer(new ClientConfig());
        try {
            consumer.subscription("test", new MessageListener() {
                @Override
                public boolean listener(Message message) {
                    System.out.println(new String(message.getBody(), Charset.defaultCharset()));
                    return true;
                }
            });
            consumer.start();
//            consumer.start();
        }finally {
//            consumer.close();
        }
    }
}
