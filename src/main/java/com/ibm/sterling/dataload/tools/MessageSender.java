package com.ibm.sterling.dataload.tools;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

public class MessageSender {

	private static Logger log = Logger.getLogger(MessageSender.class);

	public static void main(String[] args) throws Exception {

		log.info("Sending test message");
		send("tcp://localhost:61616", "Hello", "TEST_QUEUE");

	}

	// This was initially written to run local tests using ActiveMQ.
	// TODO : Enhance MessageSender to connect to IBM OMoC MQ over SSL and send a
	// message
	public static void send(String providerURL, String messageText, String queueName) throws Exception {

		// Create a ConnectionFactory
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(providerURL);

		// Create a Connection
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// Create a Session
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Create the destination (Topic or Queue)
		Destination destination = session.createQueue(queueName);

		// Create a MessageProducer from the Session to the Topic or Queue
		MessageProducer producer = session.createProducer(destination);

		// Create a messages
		TextMessage message = session.createTextMessage(messageText);

		producer.send(message);

		// Clean up
		session.close();
		connection.close();

	}

}
