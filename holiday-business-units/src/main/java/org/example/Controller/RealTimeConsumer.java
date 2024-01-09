package org.example.Controller;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class RealTimeConsumer implements Consumer {
    private final Connection connection;
    private final String clientID = "business-units-";
    private final Session session;

    private final String topicNameWeather = "prediction.Weather";
    private final String topicNameHotel = "supplier.Hotel";

    public RealTimeConsumer(String url) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connection = connectionFactory.createConnection();
        connection.setClientID(clientID);
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public void start(DatabaseStore databaseStore) {
        try {
            Topic destination = session.createTopic(topicNameWeather);

            MessageConsumer durableSubscriber = session.createDurableSubscriber(destination,clientID+topicNameWeather);

            durableSubscriber.setMessageListener(message -> {
                if (message instanceof TextMessage) {
                    try {
                        databaseStore.weatherStore(((TextMessage) message).getText());
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }

        try {
            Topic destination = session.createTopic(topicNameHotel);

            MessageConsumer durableSubscriber = session.createDurableSubscriber(destination,clientID+topicNameHotel);

            durableSubscriber.setMessageListener(message -> {
                if (message instanceof TextMessage) {
                    try {
                        databaseStore.hotelStore(((TextMessage) message).getText());
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (JMSException e) {
            throw new RuntimeException(e);

        }
    }
}

