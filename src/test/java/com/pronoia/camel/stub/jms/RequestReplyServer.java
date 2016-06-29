package com.pronoia.camel.stub.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestReplyServer extends Thread {
    Logger log = LoggerFactory.getLogger(this.getClass());

    ConnectionFactory connectionFactory;
    String requestQueueName;
    String userName;
    String userPassword;
    long receiveTimeout = 1000;

    boolean useMessageIDAsCorrelationID = false;

    // Internal members
    Connection connection;
    Session session;
    MessageConsumer consumer;

    @Override
    public void start() {
        try {
            if (null == userName) {
                connection = connectionFactory.createConnection();
            } else {
                connection = connectionFactory.createConnection(userName, userPassword);
            }

            final boolean transacted = false;
            session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
            consumer = session.createConsumer(session.createQueue(requestQueueName));
            connection.start();
        } catch (Exception startEx) {
            throw new RuntimeException( "Error in JMS startup", startEx);
        }

        super.start();
    }

    @Override
    public void interrupt() {
        super.interrupt();

        if (null != consumer) {
            try {
                consumer.close();
            } catch (JMSException jmsEx) {
                // Eat this
            } finally {
                consumer = null;
            }
        }

        if (null != connection) {
            try {
                connection.close();
            } catch (JMSException jmsEx) {
                // Eat this
            } finally {
                connection = null;
            }
        }

        if (null != session) {
            try {
                session.close();
            } catch (JMSException jmsEx) {
                // Eat this
            } finally {
                session = null;
            }
        }
    }

    @Override
    public void run() {
        while (isAlive() && !isInterrupted()) {
            try {
                Message request = consumer.receive(receiveTimeout);
                if (null == request) {
                    log.info( "Waiting for message");
                } else {
                    TextMessage response = session.createTextMessage("Response Message");
                    Destination replyTo = request.getJMSReplyTo();
                    if (null != replyTo) {
                        String correlationID = request.getJMSCorrelationID();
                        if (null != correlationID) {
                            response.setJMSCorrelationID(correlationID);
                        } else if (useMessageIDAsCorrelationID) {
                            response.setJMSCorrelationID( request.getJMSMessageID());
                        }

                        MessageProducer producer = session.createProducer(replyTo);
                        producer.send(response);
                        producer.close();
                    }
                }
            } catch (JMSException jmsEx) {

            }
        }
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public String getRequestQueueName() {
        return requestQueueName;
    }

    public void setRequestQueueName(String requestQueueName) {
        this.requestQueueName = requestQueueName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPassword() {
        return userPassword;
    }

    public void setUserPassword(String userPassword) {
        this.userPassword = userPassword;
    }

    public boolean isUseMessageIDAsCorrelationID() {
        return useMessageIDAsCorrelationID;
    }

    public void setUseMessageIDAsCorrelationID(boolean useMessageIDAsCorrelationID) {
        this.useMessageIDAsCorrelationID = useMessageIDAsCorrelationID;
    }
}
