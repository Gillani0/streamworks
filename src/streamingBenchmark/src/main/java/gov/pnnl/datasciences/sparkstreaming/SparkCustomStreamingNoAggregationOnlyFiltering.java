package gov.pnnl.datasciences.sparkstreaming;

import java.io.BufferedReader;
import java.io.DataOutputStream;
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

public class SparkCustomStreamingNoAggregationOnlyFiltering extends Receiver<String> {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	static JavaStreamingContext ssc;
	static ReceiverSupervisor localReceiverSupervisor = null;
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
    if (args.length < 5) {
      System.err.println("Usage: JavaCustomReceiver <hostname> <port> <inputFileName> <RunTimesFile> <TimeWindow>");
      System.exit(1);
    }
    
    for (String arg: args){
    	System.out.println(arg);
    }
    
    PrintWriter writer = new PrintWriter(args[3], "UTF-8");

    SparkConf sparkConf = new SparkConf().setAppName("SparkCustomStreamingNoAggregationOnlyFiltering");
    ssc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(args[4])));

    
    System.out.println("created context: ");
    SparkCustomStreamingNoAggregationOnlyFiltering reciever = new SparkCustomStreamingNoAggregationOnlyFiltering(args[0], Integer.parseInt(args[1]), args[2], args[3], args[5], Integer.parseInt(args[6]));
    
    JavaReceiverInputDStream<String> lines = ssc.receiverStream(reciever
      );
    
    System.out.println("after recieve stream");
    
    
    
    localReceiverSupervisor = new ReceiverSupervisor(reciever,sparkConf ){

    	@Override
    	public void pushArrayBuffer(ArrayBuffer<?> arg0, Option<Object> arg1,
    			Option<StreamBlockId> arg2) {
    		// TODO Auto-generated method stub
    		
    	}

    	@Override
    	public void pushBytes(ByteBuffer arg0, Option<Object> arg1,
    			Option<StreamBlockId> arg2) {
    		// TODO Auto-generated method stub
    		
    	}

    	@Override
    	public void pushIterator(Iterator<Object> arg0, Option<Object> arg1,
    			Option<StreamBlockId> arg2) {
    		// TODO Auto-generated method stub
    		
    	}

    	@Override
    	public void pushSingle(Object arg0) {
    		// TODO Auto-generated method stub
    		
    	}

    	@Override
    	public void reportError(String arg0, Throwable arg1) {
    		// TODO Auto-generated method stub
    		
    	}
    	
    };
   

   

    
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      
      public Iterable<String> call(String x) {
        return Lists.newArrayList(x);
      }
    });
   

    System.out.println("after call function");
    
  	  
  	 JavaDStream<String> UDP_Flows = words.filter(
  			new Function<String, Boolean>() {
  			public Boolean call(String word) { return word.contains("UDP"); }
  			}
  			);
  	 JavaDStream<String> TCP_Flows = words.filter(
   			new Function<String, Boolean>() {
   			public Boolean call(String word) { return word.contains("TCP"); }
   			}
   			);
  	 JavaDStream<String> ICMP_Flows = words.filter(
    			new Function<String, Boolean>() {
    			public Boolean call(String word) { return word.contains("ICMP"); }
    			}
    			);
  	JavaDStream<String> FTP_Flows = words.filter(
			new Function<String, Boolean>() {
			public Boolean call(String word) { return word.contains("FTP"); }
			}
			);
  	
  	
  	 JavaDStream<String> toStop = words.filter(
 			new Function<String, Boolean>() {
 			public Boolean call(String word) { 
 			//	stop();
 				System.out.println("Stopped the SSC");
 			  
 				return word.contains("XXXXXX"); }
 			}
 			);
  	JavaPairDStream<String, Integer> toStop_Counts = toStop.mapToPair(
	  	      new PairFunction<String, String, Integer>() {
	  	    	
	  	        public Tuple2<String, Integer> call(String s) {
	  	        	System.out.println("INSIDE MAP XWWWWWWWWWWWWWWWWWWWWWW: "+s);
	  	        	String [] pieces = s.split(" +");
	  	        	if(pieces.length>8){
	  	        		System.out.println("String:\t"+s);
//	  	        		Socket messagingSocket;
//						try {
//							messagingSocket = SparkCustomStreamingNoAggregationOnlyFiltering.messagingSocket;
//							DataOutputStream  os = new DataOutputStream(messagingSocket.getOutputStream());
//							os.writeUTF("Processing the last RDF containing XXX at time: "+System.currentTimeMillis()+"\n");
//							os.flush();
//							os.close();
//						//	messagingSocket.close();
//						} catch (UnknownHostException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
	  	        	//	stop();
	  	        	//	System.exit(0);
	  	        		Throwable throwable = new Throwable();
	  	        		Option<Throwable> optionThrowable = Option.apply(throwable);
	  	        		    localReceiverSupervisor.stopReceiver("Stopping reciever via supervisor: "+System.currentTimeMillis(), optionThrowable);
	  	          return new Tuple2<String, Integer>(pieces[4]+" - "+pieces[6], Integer.valueOf(pieces[8]));
	  	        	}
	  	        	else{
	  	        		System.out.println("Exit time: "+ System.currentTimeMillis());
	  	        		//System.exit(0);
//	  	        		Socket messagingSocket;
//						try {
//							messagingSocket = SparkCustomStreamingNoAggregationOnlyFiltering.messagingSocket;
//							DataOutputStream  os = new DataOutputStream(messagingSocket.getOutputStream());
//							os.writeUTF("Processing the last RDF containing XXX at time: "+System.currentTimeMillis()+"\n");
//							os.flush();
//							os.close();
//		//					messagingSocket.close();
//						} catch (UnknownHostException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//	  	        		stop();
//	  	        		System.exit(0);
	  	        		Throwable throwable = new Throwable();
	  	        		Option<Throwable> optionThrowable = Option.apply(throwable);
	  	        		    localReceiverSupervisor.stopReceiver("Stopping reciever via supervisor: "+System.currentTimeMillis(), optionThrowable);
	  	         
	  	        		return new Tuple2<String, Integer>("incompleteBuffer", 1);
	  	    	        
	  	        	}
	  	        }
	  	      });
  
  	toStop_Counts.print();
  	
  	 JavaPairDStream<String, Integer> UDP_FLOW_Counts = UDP_Flows.mapToPair(
  	  	      new PairFunction<String, String, Integer>() {
  	  	    	  
  	  	        public Tuple2<String, Integer> call(String s) {
  	  	        	System.out.println("INSIDE MAP: "+s);
  	  	        	String [] pieces = s.split(" +");
  	  	        	if(pieces.length>8){
  	  	          return new Tuple2<String, Integer>(pieces[4]+" - "+pieces[6], Integer.valueOf(pieces[8]));
  	  	        	}
  	  	        	else{
  	  	        		return new Tuple2<String, Integer>("incompleteBuffer", 1);
  	  	    	        
  	  	        	}
  	  	        }
  	  	      });
//  	  	      
//  	JavaPairDStream<String, Integer> UDP_FLOW_Counts_SmallTimeSlot= UDP_FLOW_Counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
//  	  	        
//  	  	        public Integer call(Integer i1, Integer i2) {
//  	  	        	System.out.println("INSIDE REDUCE: ");
//  	  	          return i1 + i2;
//  	  	        }
//  	  	      });
//  	  	      
////  	JavaPairDStream<String, Integer> UDP_FLOW_Counts_LargeTimeSlot = UDP_FLOW_Counts.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
////	  	        
////	  	        public Integer call(Integer i1, Integer i2) {
////	  	        	System.out.println("INSIDE REDUCE: ");
////	  	          return i1 + i2;
////	  	        }
////	  	      }, new Duration(60000),new Duration(1000));
//
// // 	  	    UDP_FLOW_Counts.print();
//
//  	
//  	UDP_FLOW_Counts_SmallTimeSlot.foreach(
//  			new Function<JavaPairRDD<String, Integer>, Void> () {
//  			public Void call(JavaPairRDD<String, Integer> rdd) {
//  			String out = "\nUDP Flows 1 sec integration:\n";
//  			for (Tuple2<String, Integer> t: rdd.take(10)) {
//  			out = out + t.toString() + "\n";
//  			}
//  			System.out.println(out);
//  			return null;
//  			}
//  			}
//  			);
////  	UDP_FLOW_Counts_LargeTimeSlot.foreach(
////  			new Function<JavaPairRDD<String, Integer>, Void> () {
////  			public Void call(JavaPairRDD<String, Integer> rdd) {
////  			String out = "\nUDP Flows 1 minute integration:\n";
////  			for (Tuple2<String, Integer> t: rdd.take(10)) {
////  			out = out + t.toString() + "\n";
////  			}
////  			System.out.println(out);
////  			return null;
////  			}
////  			}
////  			); 	
//  	
  	 
  	UDP_FLOW_Counts.print();
  	  	 JavaPairDStream<String, Integer> TCP_FLOW_Counts = TCP_Flows.mapToPair(
  	  	  	      new PairFunction<String, String, Integer>() {
  	  	  	    	  
  	  	  	        public Tuple2<String, Integer> call(String s) {
  	  	  	        	System.out.println("INSIDE MAP: "+s);
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
  	  	  	          return new Tuple2<String, Integer>(key, Integer.valueOf(val));
  	  	  	        	}
  	  	  	        	else{
  	  	  	        		return new Tuple2<String, Integer>("incompleteBuffer", 1);
  	  	  	    	        
  	  	  	        	}
  	  	  	        }
  	  	  	      });
  	 
//  	 .reduceByKey(new Function2<Integer, Integer, Integer>() {
//  	  	  	        
//  	  	  	        public Integer call(Integer i1, Integer i2) {
//  	  	  	        	System.out.println("INSIDE REDUCE: ");
//  	  	  	          return i1 + i2;
//  	  	  	        }
//  	  	  	      });
//
//
  	  	 
  	  	TCP_FLOW_Counts.print();
  	  	 
//  	  	
//  	  	  	TCP_FLOW_Counts.foreach(
//  	  			new Function<JavaPairRDD<String, Integer>, Void> () {
//  	  			public Void call(JavaPairRDD<String, Integer> rdd) {
//  	  			String out = "\nTCP Flows:\n";
//  	  			for (Tuple2<String, Integer> t: rdd.take(10)) {
//  	  			out = out + t.toString() + "\n";
//  	  			}
//  	  			System.out.println(out);
//  	  			return null;
//  	  			}
//  	  			}
//  	  			);
//  	  	  	
  	   	 JavaPairDStream<String, Integer> ICMP_FLOW_Counts = ICMP_Flows.mapToPair(
  	  	  	      new PairFunction<String, String, Integer>() {
  	  	  	    	  
  	  	  	        public Tuple2<String, Integer> call(String s) {
  	  	  	        	System.out.println("INSIDE MAP: "+s);
  	  	  	        	String [] pieces = s.split(" +");
  	  	  	        	if(pieces.length>8){
  	  	  	          return new Tuple2<String, Integer>(pieces[4]+" - "+pieces[6], Integer.valueOf(pieces[8]));
  	  	  	        	}
  	  	  	        	else{
  	  	  	        		return new Tuple2<String, Integer>("incompleteBuffer", 1);
  	  	  	    	        
  	  	  	        	}
  	  	  	        }
  	  	  	      });
  	  	  	      
  	   	 
//  	  	  	      .reduceByKey(new Function2<Integer, Integer, Integer>() {
//  	  	  	        
//  	  	  	        public Integer call(Integer i1, Integer i2) {
//  	  	  	        	System.out.println("INSIDE REDUCE: ");
//  	  	  	          return i1 + i2;
//  	  	  	        }
//  	  	  	      });
//
//
  	   	 
  	   ICMP_FLOW_Counts.print();
  	   	 
//  	  	
//  	  	  	ICMP_FLOW_Counts.foreach(
//  	  			new Function<JavaPairRDD<String, Integer>, Void> () {
//  	  			public Void call(JavaPairRDD<String, Integer> rdd) {
//  	  			String out = "\nICMP Flows:\n";
//  	  			for (Tuple2<String, Integer> t: rdd.take(10)) {
//  	  			out = out + t.toString() + "\n";
//  	  			}
//  	  			System.out.println(out);
//  	  			return null;
//  	  			}
//  	  			}
//  	  			);
//  	 
  	   	 
  	   	 
  	   	 
  	  	
  	  	long startTime = System.currentTimeMillis();
  	    
  	    System.out.println("Start time of the experiment: "+ startTime);
  	    writer.write("StartTime: "+startTime+"\n");
  	    writer.flush();
    ssc.start();
    reciever.attachExecutor(localReceiverSupervisor); 
    System.out.println("Current time of the experiment: "+ startTime);
    ssc.awaitTermination();
   
    long endTime = System.currentTimeMillis();
    writer.write("endTime: "+endTime+"\n");
	    System.out.println("End time of the experiment: "+ endTime);
	    writer.write("totalTime: "+(endTime-startTime)+"\n");
	    writer.close();
	    System.out.println("Total time taken in milli secs: "+ (endTime-startTime));
    
  }
	
	

	// ============= Receiver code that receives data over a socket
	// ==============

	String host = null;
	int port = -1;
	
	 String mHost = null;
	 int mPort = -1;

	String outputFileName = null;
	String inputFileName = null;
	static Socket messagingSocket;
	public SparkCustomStreamingNoAggregationOnlyFiltering(String host_, int port_, String inputFile, String outFile, String messageHost, int messagePort) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
		inputFileName = inputFile;
		outputFileName = outFile;
		
		this.mHost = messageHost;
		this.mPort = messagePort;
		try {
			this.messagingSocket =  new Socket(this.mHost, this.mPort);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	Thread t;
	public void onStart() {
		// Start the thread that receives data over a connection
		 t = new Thread() {
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
		try {
			socket.shutdownInput();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	Socket socket = null;
	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		
		String userInput = null;

		
		try {
			// connect to the server
			socket = new Socket(host, port);
//			Path pt = new Path(
//					"file:///home/hduser/netflow-sample.txt");
			System.out.println("Inside recive function");
			Path pt = new Path("file://"+inputFileName);

			Configuration conf = new Configuration();
			FileSystem fs = pt.getFileSystem(conf);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			long startTime = System.currentTimeMillis();
			System.out.println("Messaging port Details: "+ this.mHost+":"+this.mPort);
//			Socket messagingSocket =  new Socket(this.mHost, this.mPort);
//			DataOutputStream  os = new DataOutputStream(messagingSocket.getOutputStream());
//	  	    
	  	    System.out.println("Start time of the experiment: "+ startTime);
//	  	    os.writeUTF("Streaming start time: "+ startTime+"milliSecs \n");
//	  	    os.flush();
//			// Until stopped or connection broken continue reading
			while (!isStopped() && ((userInput = in.readLine()) != null)) {
				System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
			in.close();
//			 os.writeUTF("Finished putting things on queue: "+ System.currentTimeMillis()+"millisecs\n");
//			 os.writeUTF("Total time taken to put on queue: "+ (System.currentTimeMillis() -startTime)+"millisecs\n");
//		  	    os.flush();
//		  	    os.close();
//		  	    messagingSocket.close();
			
//			Runtime.getRuntime().addShutdownHook(new Thread() {
//				@Override
//				public void run(){
//					System.out.println("Inside hook asking to stop");
//					ssc.stop();
//					
//				}
//			});
			
//			t.stop();
			socket.close();
			ssc.stop(true,true);
//			stop("Done streaming");
			System.out.println("Out of while loop: isStopped :"+isStopped() +"userInput :"+ userInput);
		
		//	System.exit(5);
			 long endTime = System.currentTimeMillis();
			 System.out.println("Total time taken in milli secs: "+ (endTime-startTime));
			// System.exit(0);
			// Restart in an attempt to connect again when server is active
			// again
		//	restart("Trying to connect again");
		} catch (ConnectException ce) {
			
			System.out.println(ce);
			// restart if could not connect to server
		//	restart("Could not connect", ce);
		} catch (Throwable t) {
			System.out.println(t);
		//	restart("Error receiving data", t);
		}
	}
	
	private static void stop(){
		System.out.println("Stopping the ssc..");
		
		ssc.stop(true,true);
	}
}
