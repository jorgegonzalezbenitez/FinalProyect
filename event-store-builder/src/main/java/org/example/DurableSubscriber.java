package org.example;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class DurableSubscriber implements Subscriber {
    private final Connection connection;
    private final String clientID = "datalake-builder-";
    private final Session session;

    private final String topicNameWeather = "prediction.Weather";
    private final String topicNameHotel = "supplier.Hotel";

    public DurableSubscriber(String url) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connection = connectionFactory.createConnection();
        connection.setClientID(clientID);
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public void start(Listener listener) {
        try {
            Topic destination = session.createTopic(topicNameWeather);

            MessageConsumer durableSubscriber = session.createDurableSubscriber(destination, clientID + topicNameWeather);

            durableSubscriber.setMessageListener(message -> {
                if (message instanceof TextMessage) {
                    try {
                        listener.consume(((TextMessage) message).getText(), topicNameWeather);
                    } catch (JMSException | JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }

        try {
            Topic destination = session.createTopic(topicNameHotel);

            MessageConsumer durableSubscriber = session.createDurableSubscriber(destination, clientID + topicNameHotel);

            durableSubscriber.setMessageListener(message -> {
                if (message instanceof TextMessage) {
                    try {
                        listener.consume(((TextMessage) message).getText(), topicNameHotel);
                    } catch (JMSException | JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (JMSException e) {
            throw new RuntimeException(e);

        }
    }
}

