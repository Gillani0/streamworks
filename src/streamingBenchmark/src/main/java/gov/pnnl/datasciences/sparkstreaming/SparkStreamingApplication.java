
package gov.pnnl.datasciences.sparkstreaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.activemq.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StreamBlockId;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.streaming.receiver.ReceiverSupervisor;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;

import com.google.common.collect.Lists;

public class SparkStreamingApplication extends Receiver<String> {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	static JavaStreamingContext ssc;
	static ActiveMQProducer activeMQProducer = null;
	static Boolean isFirstMessage = true;
	static String activeMqService,activeMQ_Queue =null;
	
	static String host = null;
	static int port = -1;
		
	static String inputFileName = null;
	static Socket socket = null;
	static Thread t;
	
	public SparkStreamingApplication(String host_, int port_, String inputFile, String activeMqService, String activeMQ_Queue) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		
		
		this.host = host_;
		this.port = port_;
		this.inputFileName = inputFile;
		this.activeMqService=activeMqService;
		this.activeMQ_Queue =activeMQ_Queue;
		
		this.activeMQProducer = ActiveMQProducer.createInstance(activeMqService,activeMQ_Queue);
	
		
	}
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
    if (args.length < 6) {
      System.err.println("Usage: SparkAllPassFilter <hostname> <port> <inputFileName><TimeWindow> <activeMQAddress> <activeMQ_Queue>");
      System.exit(1);
    }
    
    for (String arg: args){
    	System.out.println(arg);
    }
    SparkConf sparkConf = new SparkConf().setAppName("SparkAllPassFilter");
    ssc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(args[3])));

    System.out.println("created context: ");
    SparkStreamingApplication reciever = new SparkStreamingApplication(args[0], Integer.parseInt(args[1]), args[2], args[4], args[5]);
    
    JavaReceiverInputDStream<String> lines = ssc.receiverStream(reciever);

   
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      
      public Iterable<String> call(String x) {
    	  
    	  
    	  if(!x.isEmpty()&&isFirstMessage){
        		
        		isFirstMessage = false;
        		

        		System.out.println("Sending first message to ActiveMQ");
        		
        		if(SparkAllPassFilter.activeMQProducer!=null){
        		
        		SparkAllPassFilter.activeMQProducer.sendMessage("::MESSAGE::BEGIN COUNTING");
        		}
        		else{
        			
        			
        			SparkAllPassFilter.activeMQProducer = ActiveMQProducer.createInstance(Conf.ACTIVE_MQ_SERVICE_ADDRESS, Conf.ACTIVE_MQ_QUEUE);
        		}
        		System.out.println("First Message sent to ActiveMQ");
        	}
    	  
        return Lists.newArrayList(x);
      }
    });
   

    System.out.println("after call function");
    
  	  
  	 JavaDStream<String> allFlows = words.filter(
  			new Function<String, Boolean>() {
  			public Boolean call(String word) { return Boolean.TRUE; }
  			});
  	JavaPairDStream<String, Integer> allFlowsMap = allFlows.mapToPair(
	  	      new PairFunction<String, String, Integer>() {
	  	    	  
	  	        public Tuple2<String, Integer> call(String s) {
	  	       
	  	          return new Tuple2<String, Integer>(s, 1);
	  	        	
	  	     
	  	        }
	  	      });
  	 
  	allFlowsMap.foreachRDD(
  			new Function<JavaPairRDD<String, Integer>, Void> () {
  			public Void call(JavaPairRDD<String, Integer> rdd) {
  			
  				java.util.Iterator<Tuple2<String, Integer>> localIte = rdd.toLocalIterator();
  				
  				while(localIte.hasNext()){

  					
  					SparkAllPassFilter.activeMQProducer.sendMessage(localIte.next().toString());
  					
  				}
  				
  				return null;
  			
  			}
  			}
  			);
  	 
  	
   ssc.start();
   ssc.awaitTermination();
   SparkAllPassFilter.activeMQProducer.cleanUp();
  }
	
	

	// ============= Receiver code that receives data over a socket
	// ==============


	
	
	
	public void onStart() {
		// Start the thread that receives data over a connection
		SparkAllPassFilter.t = new Thread() {
			@Override
			public void run() {
				
				receive();
			}
		};
	
		t.start();

		
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
		

	}
	
	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		
		String userInput = null;

		
		try {
			

			System.out.println("Inside recive function");
			Path pt = new Path("file://"+Conf.FILE_BEING_PROCESSED);

			Configuration conf = new Configuration();
			FileSystem fs = pt.getFileSystem(conf);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			
			
			ActiveMQProducer activeMQProducerLocal = ActiveMQProducer.createInstance(Conf.ACTIVE_MQ_SERVICE_ADDRESS,Conf.ACTIVE_MQ_QUEUE);
				
	
			ActiveMQProducer.sendMessage("::MESSAGE::Start time of stream: "+System.currentTimeMillis());
			int count =0;
	  	    while (!isStopped() && ((userInput = in.readLine()) != null) && (count < Conf.MAX_LINES)) {
				//System.out.println("Received data '" + userInput + "'");
				store(userInput);
				count++;
			}
	  	  ActiveMQProducer.sendMessage("::MESSAGE::End time of stream: "+System.currentTimeMillis()); 
			in.close();

			ActiveMQProducer.sendMessage("::MESSAGE::#Shutting down local producer inside recieve");
		} catch (ConnectException ce) {
			
			System.out.println(ce);
		} catch (Throwable t) {
			System.out.println(t);
		}
	}
	

}
