package gov.pnnl.datasciences.sparkstreaming;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

public class TestActiveMQ_ProducerConsumer {

	
	public static void main(String args[]) throws JMSException, NumberFormatException{
		
		
		ActiveMQProducer producer =  ActiveMQProducer.createInstance("tcp://192.168.0.1:61616", "allPassQueue");
//		ActiveMQConsumer consumer =  new ActiveMQConsumer("tcp://192.168.0.1:61616", "allPassQueue","times.txt","XXXX");
//		consumer.setUpConsumer();
	//	consumer.bringConsumerAlive(consumer);
//		consumer.listenForMessages();
//		MessageProducer mp = producer.setUpProducer();
		

		
		ActiveMQProducer.sendMessage("Hello");

		ActiveMQProducer.sendMessage("Hello Again");
		
		ActiveMQProducer.sendMessage("XXXX");
		
		
	producer.cleanUp();
	System.exit(0);
	//	consumer.cleanUp();
		
		
	}
	
	
}
