package gov.pnnl.datasciences.sparkstreaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.Socket;

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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import scala.Serializable;
import scala.Tuple2;

import com.google.common.collect.Lists;

public class SparkAllPassFilter extends Receiver<String>  {
	/**
	 * 
	 */
	
	static JavaStreamingContext ssc;
	 static ActiveMQProducer activeMQProducer ;
	 static Boolean isFirstMessage = true;
	 static String activeMqService,activeMQ_Queue;
	
	 String host = null;
	 int port = -1;
		
	 String inputFileName = null;
	 Socket socket = null;
	 Thread t;
	 static SparkAllPassFilter classObj =null;
	 int MAX_LINES_TO_READ = 0;
	
	public  static SparkAllPassFilter getClassObj() {
		return SparkAllPassFilter.classObj;
	}


	private  SparkAllPassFilter(String host_, int port_, String inputFile, String activeMqService, String activeMQ_Queue, int MAX_LINES) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		
		
		this.host = host_;
		this.port = port_;
		this.inputFileName = inputFile;
		this.activeMqService=activeMqService;
		this.activeMQ_Queue =activeMQ_Queue;
		
		SparkAllPassFilter.activeMQProducer = ActiveMQProducer.createInstance(activeMqService,activeMQ_Queue);
	
		this.MAX_LINES_TO_READ=MAX_LINES;
		
		
		
	}
	
	
	public  static SparkAllPassFilter getInstance(String host_, int port_, String inputFile, String activeMqService, String activeMQ_Queue, int MAX_LINES){
		
		if(SparkAllPassFilter.classObj==null){
			return new SparkAllPassFilter( host_,  port_,  inputFile,  activeMqService,  activeMQ_Queue, MAX_LINES);
		}
		else{
			return SparkAllPassFilter.classObj;
		}
		
	}
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
    if (args.length < 6) {
      System.err.println("Usage: SparkAllPassFilter <hostname> <port> <inputFileName><TimeWindow> <activeMQAddress> <activeMQ_Queue>");
      System.exit(1);
    }
    
    for (String arg: args){
    	System.out.println(arg);
    }
    
    SparkAllPassFilter.classObj = SparkAllPassFilter.getInstance(args[0], Integer.parseInt(args[1]), args[2], args[4], args[5], Integer.parseInt(args[6]));
    
    
    
    SparkConf sparkConf = new SparkConf().setAppName("SparkAllPassFilter");
    ssc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(args[3])));
    
    System.out.println("created context: ");
   
    JavaReceiverInputDStream<String> lines = ssc.receiverStream(SparkAllPassFilter.classObj);

   
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      
      /**
		 * 
		 */
		

	public Iterable<String> call(String x) {
    	  
    	  
    	  if(!x.isEmpty()&&isFirstMessage){
        		
    		 isFirstMessage = false;
        		

        		System.out.println("Sending first message to ActiveMQ");
        		
        		if(activeMQProducer!=null){
        		
        			activeMQProducer.sendMessage("::MESSAGE::BEGIN COUNTING");
        		}
        		else{
        			System.out.println("Something went wrong...as producer is not instantiated");
        			
        			activeMQProducer = ActiveMQProducer.createInstance(activeMqService, activeMQ_Queue);
        		}
        		System.out.println("First Message sent to ActiveMQ");
        	}
    	  
        return Lists.newArrayList(x);
      }
    });
   

    System.out.println("after call function");
    
  	  
  	 JavaDStream<String> allFlows = words.filter(
  			new Function<String, Boolean>() {
  			/**
				 * 
				 */
				

			public Boolean call(String word) { return Boolean.TRUE; }
  			});
  	 
  	 
  	JavaPairDStream<String, Integer> allFlowsMap = allFlows.mapToPair(
	  	      new PairFunction<String, String, Integer>() {
	  	    	  
//	  	        public Tuple2<String, Integer> call(String s) {
//	  	       
//	  	          return new Tuple2<String, Integer>(s, 1);
//	  	        	
//	  	     
//	  	        }
	  	    	  
	  	    	 /**
				 * 
				 */
				

				public Tuple2<String, Integer> call(String s) {
//	  	  	        	System.out.println("INSIDE MAP: "+s);
	  	  	        	String [] pieces = s.split(" +");
	  	  	        	if(pieces.length>8){
	  	  	        		
	  	  	        		String key = pieces[4]+" - "+pieces[6];
	  	  	        		String val = pieces[8];
	  	  	      
	  	  	        		if(pieces[9].equalsIgnoreCase("M")){
	  	  	        			
	  	  	        			float actualVal = Float.valueOf(val)*1000000;
	  	  	        			val = String.valueOf((int)actualVal);
	  	  	        			
	  	  	        		}
	  	  	        		else if(pieces[9].equalsIgnoreCase("G")){
	  	  	        		float actualVal = Float.valueOf(val)*1000000000;
	  	  	        			val = String.valueOf(actualVal);
	  	  	        		}
	  	  	        		else if(pieces[8].equalsIgnoreCase("bytes")){
	  	  	        			val = "0";
	  	  	        		}
	  	  	        		else{
	  	  	        			try{
	  	  	        			 Integer.valueOf(val);
	  	  	        			}catch(NumberFormatException e){
	  	  	        				System.out.println("caught number format exception for val ="+val);
	  	  	        				val = "0";
	  	  	        			}
	  	  	        		}
	  	  	          return new Tuple2<String, Integer>(key, Integer.valueOf(val));
	  	  	        	}
	  	  	        	else{
	  	  	        		return new Tuple2<String, Integer>("incompleteBuffer", 1);
	  	  	    	        
	  	  	        	}
	  	  	        }
	  	  	      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	  	  	        
	  	  	        /**
					 * 
					 */
					

					public Integer call(Integer i1, Integer i2) {
	  	  	      // 	System.out.println("INSIDE REDUCE: ");
						
						if(activeMQProducer !=null){
	  	  	        activeMQProducer.sendMessage("Inside reduce Function");
						}else{
							System.out.println("Same f problem: u r idiot");
						}

	  	  	          return i1 + i2;
	  	  	        }
	  	    	  
	  	    	  
	  	      });
  	 
  	allFlowsMap.foreachRDD(
  			new Function<JavaPairRDD<String, Integer>, Void> () {
  			/**
				 * 
				 */
				

			public Void call(JavaPairRDD<String, Integer> rdd) {
  			
  				java.util.Iterator<Tuple2<String, Integer>> localIte = rdd.toLocalIterator();
  				
  				while(localIte.hasNext()){

  					
  					activeMQProducer.sendMessage(localIte.next().toString());
  					
  				}
  				
  				return null;
  			
  			}
  			}
  			);
  	 
  	System.out.println("About to start..");
  	ssc.start();
  	System.out.println("Just started..");
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	ssc.awaitTermination();
  	SparkAllPassFilter.classObj.activeMQProducer.cleanUp();
  }
	
	

	// ============= Receiver code that receives data over a socket
	// ==============


	
	
	
	public void onStart() {
		// Start the thread that receives data over a connection
		
		System.out.println("Inside onstart() : class obj null? = "+ (classObj==null));
		t =  new Thread() {
			@Override
			public void run() {
				System.out.println("Calling recieve()");
				receive();
			}
		};
	
//		try {
//			t.sleep((long)1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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
			System.out.println("inputFile URI: file://"+inputFileName);
			Path pt = new Path("file://"+inputFileName);

			Configuration conf = new Configuration();
			FileSystem fs = pt.getFileSystem(conf);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			
			
	//		ActiveMQProducer activeMQProducerLocal = ActiveMQProducer.createInstance(Conf.ACTIVE_MQ_SERVICE_ADDRESS,Conf.ACTIVE_MQ_QUEUE);
			
			
	//		ActiveMQProducer activeMQProducerLocal = ActiveMQProducer.createInstance(activeMqService,activeMQ_Queue);
			
	
			ActiveMQProducer.sendMessage("::MESSAGE::Start time of stream: "+System.currentTimeMillis());
			ActiveMQProducer.sendMessage("::MESSAGE::MaxRecords to be sent: "+MAX_LINES_TO_READ);
			int count =0;
	  	    while (!isStopped() && ((userInput = in.readLine()) != null) && (count < MAX_LINES_TO_READ)) {
		//		System.out.println("Received data '"+count +" : " + userInput + "'");
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
