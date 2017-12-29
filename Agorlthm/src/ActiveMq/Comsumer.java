package ActiveMq;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Comsumer {
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    private static final String BROKEN_URL = ActiveMQConnection.DEFAULT_BROKER_URL;
    ConnectionFactory connectionFactory;
    Connection connection;
    Session session;
    ThreadLocal<MessageConsumer> threadLocal = new ThreadLocal<MessageConsumer>();
    AtomicInteger count = new AtomicInteger();
    public void init(){
        try {
            connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,BROKEN_URL);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        }catch (JMSException e){
            e.printStackTrace();
        }
    }

    public void getMessage(String disname){
        try {
            Queue queue = session.createQueue(disname);
            MessageConsumer consumer = null;
            if (threadLocal.get() != null){
                consumer = threadLocal.get();
            }else {
                consumer = session.createConsumer(queue);
                threadLocal.set(consumer);
            }while (true){
                Thread.sleep(10000);
                TextMessage msg = (TextMessage) consumer.receive();
                if (msg != null){
                    msg.acknowledge();
                    System.out.print(Thread.currentThread().getName()+": Consumer:我是消费者，我正在消费Msg"+msg.getText()+"--->"+count.getAndIncrement());
                }else {
                    break;
                }
            }
        }catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
