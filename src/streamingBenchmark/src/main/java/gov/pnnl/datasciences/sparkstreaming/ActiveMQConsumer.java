package gov.pnnl.datasciences.sparkstreaming;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.omg.SendingContext.RunTime;


public class ActiveMQConsumer implements Runnable, ExceptionListener {

	String activeMQAddress = null;
	String queue = null;
	static MessageConsumer consumer = null;
	static Session session=null;
	static Connection connection = null;
	static Destination destination =null;
	static ActiveMQConnectionFactory connectionFactory = null;
	static int MESSAGES_LIMIT;
	static int currentMessageCount ;
	PrintWriter writer = null;
	String stopSymbol =null;
	Boolean isFirstMessage = true;
	Boolean suspended = false;
	static Boolean needsToRun = true;
	static long firstMessageTime=0;
	static String sparkSbin = null;
	static ActiveMQConsumer activeMQConsumerObj= null;
	
	public static void main(String []args){
		
		ActiveMQConsumer consumer = ActiveMQConsumer.getInstance(args[0], args[1], args[2], args[3], Integer.valueOf(args[4]), args[5]);
		consumer.sparkSbin = args[5];
		consumer.setUpConsumer();
		consumer.listenForMessages();
//		 thread(new ActiveMQConsumer(args[0], args[1], args[2], args[3]), false);
	}
	
	public static Thread thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
       return brokerThread;
        
    }
	
//	public static Thread bringConsumerAlive(ActiveMQConsumer consumer ){
//		Thread t = thread(consumer, false);
//		return t;
//	}
	
	public static ActiveMQConsumer getInstance(String activeMQAddress, String queue, String timeRecordFile, String stopSymbol, int maxMessages, String sparkKillCommand){
		
		
		if(ActiveMQConsumer.activeMQConsumerObj==null){
			ActiveMQConsumer.activeMQConsumerObj = new ActiveMQConsumer( activeMQAddress,  queue,  timeRecordFile,  stopSymbol,  maxMessages,  sparkKillCommand);
			 return ActiveMQConsumer.activeMQConsumerObj;
		}
		else{
			return ActiveMQConsumer.activeMQConsumerObj;
		}
	}
	
	public ActiveMQConsumer getClassObj(){
		return ActiveMQConsumer.activeMQConsumerObj;
	}
	
private ActiveMQConsumer(String activeMQAddress, String queue, String timeRecordFile, String stopSymbol, int maxMessages, String sparkKillCommand){
		
		this.activeMQAddress =activeMQAddress;
		this.queue = queue;
		this.stopSymbol=stopSymbol;
		ActiveMQConsumer.MESSAGES_LIMIT=maxMessages;
		this.currentMessageCount=0;
		ActiveMQConsumer.sparkSbin = sparkSbin;
		try {
			writer = new PrintWriter(timeRecordFile, "UTF-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
void suspend(){
	suspended = true;
	
}

synchronized void resume(){
	suspended = false;
	notify();
}
    public void run() {
        try {

        	// Create a ConnectionFactory
           
             
 

            // Wait for a message
            Message message = ActiveMQConsumer.consumer.receive();
            System.out.println(Thread.currentThread().getName());
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                
                if(this.isFirstMessage){
                	ActiveMQConsumer.firstMessageTime = System.currentTimeMillis();
                	writer.println("FirstMessageRecieveTime: "+ActiveMQConsumer.firstMessageTime);
                	writer.flush();
                	this.isFirstMessage = false;
                	
                	System.out.println("Recieved First Message: "+text);
                	
                }
                else if(text.contains(this.stopSymbol)){
                	writer.println("FinalMessageRecieveTime: "+(System.currentTimeMillis()-ActiveMQConsumer.firstMessageTime)/1000);
                	writer.flush();
                	Runtime rt = Runtime.getRuntime();
                	writer.close();
                	Process p;	
                	System.out.println("Killing spark streaming job by shutting down Spark");
                	ActiveMQConsumer.needsToRun = false;
        			p = rt.exec(new String[] {ActiveMQConsumer.sparkSbin+"./stop-all.sh"});
        			
        			System.out.println("Killing ActiveMQ consumer");
        		//	p.waitFor();
        			cleanUp();
        			
                    System.exit(0);
                	
                }
                else{
           //     System.out.println("Received: " + text);
                }
            } else {
                System.out.println("Received: " + message);
            }

//            synchronized(this){
//            	while(!suspended){
//            		wait();
//            		notify();
//            	}
//            }
      
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
            System.exit(0);
        }
        
    }


    public void setUpConsumer(){
    	
     
	try {
		ActiveMQConsumer.connectionFactory = new ActiveMQConnectionFactory(this.activeMQAddress);
		ActiveMQConsumer.connection = connectionFactory.createConnection();
		ActiveMQConsumer.connection.start();
		ActiveMQConsumer.connection.setExceptionListener(this);
         ActiveMQConsumer.session  = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
  
          ActiveMQConsumer.destination = session.createQueue(this.queue);

          ActiveMQConsumer.consumer = session.createConsumer(destination);
      
	} catch (JMSException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      
     
    }
    
    
    
    public void listenForMessages(){
    	while(true){
    		try{
            // Wait for a message
            Message message = ActiveMQConsumer.consumer.receive();
      //      System.out.println(Thread.currentThread().getName());
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                ActiveMQConsumer.currentMessageCount++;
                if(this.isFirstMessage){
                	ActiveMQConsumer.firstMessageTime = System.currentTimeMillis();
                	writer.println("FirstMessageRecieveTime: "+ActiveMQConsumer.firstMessageTime);
                	writer.flush();
                	this.isFirstMessage = false;
                	
                	System.out.println("Recieved First Message: "+text);
                	
                }
                if(ActiveMQConsumer.currentMessageCount == ActiveMQConsumer.MESSAGES_LIMIT){
                	
                	//text.contains(this.stopSymbol)){
                
                	writer.println("FinalMessageRecieveTime: "+(System.currentTimeMillis()-ActiveMQConsumer.firstMessageTime)/1000);
                	writer.flush();
                	Runtime rt = Runtime.getRuntime();
                	writer.close();
                	Process p;	
                	System.out.println("Killing spark  job");
                	ActiveMQConsumer.needsToRun = false;
        			p = rt.exec(new String[] {"./killPIDS_SPARK.sh"});
        			p.waitFor();
//        			System.out.println("Starting spark ");
//        			p = rt.exec(new String[] {ActiveMQConsumer.sparkSbin+"./start-all.sh"});
//        			p.waitFor();
        			
        			System.out.println("Killing ActiveMQ consumer");
        			
        			cleanUp();
        			System.out.println("About to exit..");
        			System.exit(0);
                    
                	
                }
                if(text.contains("::MESSAGE::")){
                	writer.println(text+": "+(System.currentTimeMillis()-ActiveMQConsumer.firstMessageTime)/1000);
                	writer.flush();
                	
                }
                if(ActiveMQConsumer.currentMessageCount%10000 ==0){
                	writer.println("Processed Messages Count : "+ActiveMQConsumer.currentMessageCount+": "+(System.currentTimeMillis()-ActiveMQConsumer.firstMessageTime)/1000);
                	writer.flush();
                }
                
                else{
          //      System.out.println("Received: " + text);
                }
            } else {
                System.out.println("Received: " + message);
            }

//            synchronized(this){
//            	while(!suspended){
//            		wait();
//            		notify();
//            	}
//            }
      
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    	}
    	
    	
    }
    
public void cleanUp(){
    	
    	try {
    		
    		ActiveMQConsumer.consumer.close();
			ActiveMQConsumer.session.close();
			ActiveMQConsumer.connection.close();
        	
//			Runtime rt = Runtime.getRuntime();

//			Process p;	
        	System.out.println("Finished clean up");
//        	ActiveMQConsumer.needsToRun = false;
//			p = rt.exec(new String[] {"/home/datl484/./restartActiveMQ.sh"});
			
			
			
    	} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	
    }

	public void onException(JMSException arg0) {
		// TODO Auto-generated method stub
		
	}
}