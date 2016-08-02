package sandkev;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.AbstractMessageListenerContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.jms.listener.SimpleMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;
import org.springframework.jms.support.destination.JndiDestinationResolver;
import org.springframework.jndi.JndiTemplate;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.naming.Context;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by kevin on 16/07/2016.
 */
public class JmsIntegrationTest {

    private BrokerService broker;
    private JndiTemplate jndiTemplate;
    private JmsTemplate sender;
    private ConnectionFactory targetConnectionFactory;
    private JndiDestinationResolver destinationResolver;
    private String destinationName;
    private File dataDir;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        dataDir = new File("messageStore");
        dataDir.mkdir();
        broker.setDataDirectory(dataDir.getCanonicalPath());
        //broker.setDataDirectory("messageStore");
        broker.setUseJmx(false);
        //broker.setDeleteAllMessagesOnStartup(true);
        broker.start();


/*
        broker = new BrokerService();

        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://localhost:61616"));
        broker.addConnector(connector);
        broker.start();
*/



        Properties jndiProperties = new Properties();
        jndiProperties.setProperty(Context.PROVIDER_URL, "vm:localhost?broker.persistent=true");
        jndiProperties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");

        jndiTemplate = new JndiTemplate();
        jndiTemplate.setEnvironment(jndiProperties);

        targetConnectionFactory = jndiTemplate.lookup("ConnectionFactory", ConnectionFactory.class);
        //targetConnectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        //targetConnectionFactory = new ActiveMQConnectionFactory("vm://localhost?create=false");

        destinationName = "dynamicQueues/in.trade_event";

        destinationResolver = new JndiDestinationResolver();
        destinationResolver.setJndiTemplate(jndiTemplate);

        sender = createJmsTemplate(targetConnectionFactory, destinationResolver, destinationName);

    }

    @After
    public void tearDown() throws Exception {
        try {
            FileUtils.deleteDirectory(dataDir);
            //broker.deleteAllMessages();
        } catch (Exception e) {
            //
        }
        broker.stop();
    }

    @Test
    public void canSendAndReceiveUsingSimpleListener() throws Exception {
        doTest(simpleMessageListenerContainer(targetConnectionFactory, destinationResolver, destinationName));
    }

    @Test
    public void canSendAndReceiveUsingDefaultListener() throws Exception {
        doTest(defaultMessageListenerContainer(targetConnectionFactory, destinationResolver, destinationName));
    }

    private void doTest(MessageListenerContainer listenerContainer) throws Exception {

        sender.send(session -> session.createTextMessage("hi#1"));
        int expectedNumberOfMessages = 3;
        final List<Message> receivedMessages = new ArrayList<>(expectedNumberOfMessages);
        final CountDownLatch latch = new CountDownLatch(expectedNumberOfMessages);

        startListener(listenerContainer, receivedMessages, latch);

        sender.send(session -> session.createTextMessage("hi#2"));

        //simulate failover
        broker.stop();
        //broker.setDeleteAllMessagesOnStartup(false);
        broker.start();

        sender.send(session -> session.createTextMessage("hi#3"));

        latch.await(1, TimeUnit.SECONDS);
        assertEquals("wrong number of messages received", expectedNumberOfMessages, receivedMessages.size());

    }

    private void startListener(MessageListenerContainer listenerContainer, List<Message> receivedMessages, CountDownLatch latch) {
        MessageListener messageListener = message -> {
            receivedMessages.add(message);
            latch.countDown();
        };
        MessageListenerAdapter listenerAdapter = new MessageListenerAdapter();
        listenerAdapter.setDelegate(messageListener);
        listenerAdapter.setDefaultListenerMethod("onMessage");

        listenerContainer.setupMessageListener(listenerAdapter);
        listenerContainer.start();
    }

    private MessageListenerContainer defaultMessageListenerContainer(ConnectionFactory connectionFactory, JndiDestinationResolver destinationResolver, String destinationName) {
        final DefaultMessageListenerContainer listenerContainer = new DefaultMessageListenerContainer();
        decorateListenerContainer(connectionFactory, destinationResolver, destinationName, listenerContainer);
        return listenerContainer;
    }
    private MessageListenerContainer simpleMessageListenerContainer(ConnectionFactory connectionFactory, JndiDestinationResolver destinationResolver, String destinationName) {
        final SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
        decorateListenerContainer(connectionFactory, destinationResolver, destinationName, listenerContainer);
        return listenerContainer;
    }

    private void decorateListenerContainer(ConnectionFactory connectionFactory, JndiDestinationResolver destinationResolver, String destinationName, final AbstractMessageListenerContainer listenerContainer) {
        listenerContainer.setConnectionFactory(connectionFactory);
        listenerContainer.setDestinationResolver(destinationResolver);
        listenerContainer.setDestinationName(destinationName);
        listenerContainer.setPubSubDomain(false);
        listenerContainer.setPubSubNoLocal(false);
        listenerContainer.afterPropertiesSet();
    }

    private JmsTemplate createJmsTemplate(ConnectionFactory connectionFactory, JndiDestinationResolver destinationResolver, String destinationName) {
        JmsTemplate jmsTemplate = new JmsTemplate();
        jmsTemplate.setConnectionFactory(connectionFactory);
        jmsTemplate.setDestinationResolver(destinationResolver);
        jmsTemplate.setDefaultDestinationName(destinationName);
        jmsTemplate.setPubSubDomain(false);
        //jmsTemplate.setDeliveryMode(DeliveryMode.PERSISTENT);
        jmsTemplate.afterPropertiesSet();
        return jmsTemplate;
    }

}
