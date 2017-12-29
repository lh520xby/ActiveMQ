package ActiveMq;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Producter {
    // Activemq 的默认用户名
    private static final String USERNSME = ActiveMQConnection.DEFAULT_USER;
    // Activemq 的默认密码
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    // Activemq 的链接地址
    private static final String BROKEN_RUL = ActiveMQConnection.DEFAULT_BROKER_URL;

    AtomicInteger count = new AtomicInteger(0);
    // 链接工厂
    ConnectionFactory connectionFactory;
    // 链接对象
    Connection connection;
    // 事物管理
    Session session;
    ThreadLocal<MessageProducer> threadLocal = new ThreadLocal<MessageProducer>();

    // 初始化
    public void init(){
        try {
            //创建一个链接工厂
            connectionFactory = new ActiveMQConnectionFactory(USERNSME,PASSWORD,BROKEN_RUL);
            //从工厂中创建一个链接
            connection  = connectionFactory.createConnection();
            //开启链接
            connection.start();
            //创建一个事务（这里通过参数可以设置事务的级别）
            session = connection.createSession(true,Session.SESSION_TRANSACTED);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String disname){
        try {
            // 创建一个消息队列
            Queue queue = session.createQueue(disname);
            // 消息生产者
            MessageProducer messageProducer = null;
            if (threadLocal.get() != null){
                messageProducer = threadLocal.get();
            }else {
                messageProducer = session.createProducer(queue);
                threadLocal.set(messageProducer);
            }while (true){
                Thread.sleep(10000);
                int num = count.getAndIncrement();
                // 创建一条消息
                TextMessage msg = session.createTextMessage(Thread.currentThread().getName()+"productor:测试生产者生产东西！，count:"+num);
                System.out.println(Thread.currentThread().getName()+"productor:测试生产者生产东西！,count:"+num);
                // 发送消息
                messageProducer.send(msg);
                // 提交事务
                session.commit();
            }
        }catch (JMSException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}
