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

public class SparkAllPassFilter_SutanayData extends Receiver<String> {
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
	
	public SparkAllPassFilter_SutanayData(String host_, int port_, String inputFile, String activeMqService, String activeMQ_Queue) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		
		
		SparkAllPassFilter.host = host_;
		SparkAllPassFilter.port = port_;
		SparkAllPassFilter.inputFileName = inputFile;
		SparkAllPassFilter.activeMqService=activeMqService;
		SparkAllPassFilter.activeMQ_Queue =activeMQ_Queue;
		
	//	if(SparkAllPassFilter.activeMQProducer.equals(null)){
		SparkAllPassFilter.activeMQProducer = ActiveMQProducer.createInstance(activeMqService,activeMQ_Queue);
	//	}
		System.out.println("Is activeMQ instance NULL: "+activeMQProducer.equals(null));
		System.out.println("socket address: "+SparkAllPassFilter.host+":"+SparkAllPassFilter.port );
			
		
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
    SparkAllPassFilter_SutanayData reciever = new SparkAllPassFilter_SutanayData(args[0], Integer.parseInt(args[1]), args[2], args[4], args[5]);
    
    JavaReceiverInputDStream<String> lines = ssc.receiverStream(reciever);
    
//    reciever.receive();
    System.out.println("after recieve stream");
   
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      
      public Iterable<String> call(String x) {
    	  
    	  
    	  if(!x.isEmpty()&&isFirstMessage){
        		
        		isFirstMessage = false;
        		

        		System.out.println("Sending first message to ActiveMQ");
        		SparkAllPassFilter.activeMQProducer.sendMessage("::MESSAGE::BEGIN COUNTING");
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
	  	        	System.out.println("INSIDE MAP: "+s);
	  	        	
	  	        	
	  	        	
	  	          return new Tuple2<String, Integer>(s, 1);
	  	        	
	  	     
	  	        }
	  	      });
  	 
  	allFlowsMap.foreach(
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
		System.out.println("On start()");
		t.start();
		try {
			t.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
		

	}
	
	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		
		String userInput = null;

		
		try {
			// connect to the server
			
	//		System.out.println("socket address inside recieve: "+Conf.STREAM_IP_HOST+":"+Conf.STREAM_IP_PORT);
	//		Socket socketLocal = new Socket(Conf.STREAM_IP_HOST, Conf.STREAM_IP_PORT);

			System.out.println("Inside recive function");
			Path pt = new Path("file://"+Conf.FILE_BEING_PROCESSED);

			Configuration conf = new Configuration();
			FileSystem fs = pt.getFileSystem(conf);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			
			
			ActiveMQProducer activeMQProducerLocal = ActiveMQProducer.createInstance(Conf.ACTIVE_MQ_SERVICE_ADDRESS,Conf.ACTIVE_MQ_QUEUE);
				
	
			ActiveMQProducer.sendMessage("::MESSAGE::Start time of stream: "+System.currentTimeMillis());
	  	    while (!isStopped() && ((userInput = in.readLine()) != null)) {
				System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
	  	  ActiveMQProducer.sendMessage("::MESSAGE::End time of stream: "+System.currentTimeMillis()); 
			in.close();

	//		socketLocal.close();
			ActiveMQProducer.sendMessage("::MESSAGE::#Shutting down local producer inside recieve");
			activeMQProducerLocal.cleanUp();
			
		} catch (ConnectException ce) {
			
			System.out.println(ce);
			// restart if could not connect to server
		//	restart("Could not connect", ce);
		} catch (Throwable t) {
			System.out.println(t);
		//	restart("Error receiving data", t);
		}
	}
	
//	private static void stop(){
//		System.out.println("Stopping the ssc..");
//		
//		ssc.stop(true,true);
//	}
}
