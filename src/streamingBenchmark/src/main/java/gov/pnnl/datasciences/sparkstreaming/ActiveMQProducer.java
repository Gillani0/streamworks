package gov.pnnl.datasciences.sparkstreaming;


import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import scala.Serializable;



public  class ActiveMQProducer implements Runnable, Serializable {
	
	static String activeMQAddress = null;
	static String queue = null;
	static MessageProducer messageproducer = null;
	static Session session=null;
	static Destination destination = null;
	
	private ActiveMQProducer(String activeMQAddress, String queue){
		
		ActiveMQProducer.activeMQAddress =activeMQAddress;
ActiveMQProducer.queue = queue;
	//	thread(new ActiveMQProducer(activeMQAddress, queue), false);
		
	}
	
	public static  ActiveMQProducer createInstance (String activeMQAddress, String queue){
		
		ActiveMQProducer producer =new  ActiveMQProducer(activeMQAddress, queue);
		ActiveMQProducer.messageproducer = producer.setUpProducer();
	//	ActiveMQProducer.session = producer.
	//	thread(producer, false);
		
		return producer;
		
	}
	
	 public static void thread(Runnable runnable, boolean daemon) {
	        Thread brokerThread = new Thread(runnable);
	        brokerThread.setDaemon(daemon);
	        brokerThread.start();
	    }
	
    public void run() {
        try {
     //   	ActiveMQProducer.producer = ActiveMQProducer.setUpProducer();
        	 ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQProducer.activeMQAddress);
			 Connection connection = connectionFactory.createConnection();
		
        connection.start();

        // Create a Session
        ActiveMQProducer.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);  

        // Create the destination (Topic or Queue)
        ActiveMQProducer.destination = ActiveMQProducer.session.createQueue(ActiveMQProducer.queue);

        // Create a MessageProducer from the Session to the Topic or Queue
        ActiveMQProducer.messageproducer = ActiveMQProducer.session.createProducer(ActiveMQProducer.destination);
        ActiveMQProducer.messageproducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);  
        	
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }
    
    public static MessageProducer setUpProducer(){
    	 MessageProducer producer = null;
		try {
			 ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQProducer.activeMQAddress);
			 Connection connection = connectionFactory.createConnection();
		
        connection.start();

        // Create a Session
        ActiveMQProducer.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        ActiveMQProducer.destination = ActiveMQProducer.session.createQueue(ActiveMQProducer.queue);

        // Create a MessageProducer from the Session to the Topic or Queue
        producer = ActiveMQProducer.session.createProducer(ActiveMQProducer.destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return producer;
    	
    }
    
    public static void sendMessage(String msg){
    	 // Create a messa
      TextMessage message;
	try {
		message = ActiveMQProducer.session.createTextMessage(msg);
		
		
	    ActiveMQProducer.messageproducer.send(message);
	    message.acknowledge();
	} catch (JMSException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    	
    }
    
    public void cleanUp(){
    	
    	try {
			
    		ActiveMQProducer.session.close();
    		ActiveMQProducer.messageproducer.close();
    	} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
}
