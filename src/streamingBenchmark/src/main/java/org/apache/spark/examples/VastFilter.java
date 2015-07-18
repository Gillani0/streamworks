/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import gov.pnnl.datasciences.sparkstreaming.Conf;
import gov.pnnl.datasciences.sparkstreaming.SparkAllPassFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public final class VastFilter {
	static long DOS_timeWindow1_BEGIN = 1364860800;
	static long DOS_timeWindow1_END = 1365030000;
	  
	static String [] DOS_IPs = {"10.98.107.5", "172.30.0.04", "172.20.0.4"};
	static ArrayList<String> DOS_IPs_List = new ArrayList<String> (Arrays.asList(DOS_IPs));
	 
	static long Botnet_DOS_timeWindow1_BEGIN = 1365811200;
	static long Botnet_DOS_timeWindow1_END = 1365980400;
	  
	static String [] Botnet_DOS_IPs = {"10.0.0.8", "172.20.1.47", "172.10.2.135", "172.20.1.81", "172.10.2.66", "172.30.1.223"};
	static ArrayList<String> Botnet_DOS_IPs_List = new ArrayList<String> (Arrays.asList(Botnet_DOS_IPs));
	  
	static long EXFIL_timeWindow1_BEGIN = 1365206400;
	static long EXFIL_timeWindow1_END =  1365375600;
	  
	static String [] EXFIL_IPs_victimIps = {"10.7.5.5"};
	static ArrayList<String> EXFIL_IPs_victimIpsList = new ArrayList<String>( Arrays.asList(EXFIL_IPs_victimIps));
	static HashMap<String, Long[]> EXFIL_FLOW_INFO = new HashMap<String, Long[]>();
	
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

	 
	  
    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }
    
    
    
    
	    BufferedWriter outDos = new BufferedWriter(new FileWriter("vast_netflow_filter_dos.txt"));
	    BufferedWriter outExfil = new BufferedWriter(new FileWriter("vast_netflow_filter_exfil.txt"));
	    BufferedWriter outExfil_AllTrafficDump = new BufferedWriter(new FileWriter("vast_netflow_filter_exfil_AllTrafficDump.txt"));
	    BufferedWriter outExfil_flows = new BufferedWriter(new FileWriter("vast_netflow_filter_exfil_Flows.txt"));
	    
	    BufferedWriter outBotNet = new BufferedWriter(new FileWriter("vast_netflow_filter_botNet.txt"));

	    BufferedWriter outBotNet_AllTrafficDump = new BufferedWriter(new FileWriter("vast_netflow_filter_botNet.txt"));

	

    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
    sparkConf.setMaster("local[10]");
    
    Path pt = new Path(args[0]);

	Configuration conf = new Configuration();
	FileSystem fs = pt.getFileSystem(conf);

	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//	JavaRDD<String> lines = ctx.textFile(fs.getCanonicalServiceName());
    JavaRDD<String> lines = ctx.textFile(args[0], 1);
 //   String outPath = args[1];

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String s) {
    	  Pattern NEWLINE = Pattern.compile("\n");
        return Arrays.asList(NEWLINE.split(s));
      }
    });

    JavaRDD<String> filteredExfil = words.filter(
        
    	new Function<String, Boolean>() {
  			public Boolean call(String word) { 
  				
  				String [] flowData = word.split("\\|");
  				
  				if(flowData.length>=14){
  					
  					if(flowData[0].trim().equals("")){
  						return false;
  					}
  					
  					
  					long timeInfo = Long.valueOf(flowData[0]);
  					String sourceIp = flowData[2];
  					String destinationIp = flowData[3];
  					
  					//check for Exfil
  					
  					if((VastFilter.EXFIL_timeWindow1_BEGIN < timeInfo)&&(VastFilter.EXFIL_timeWindow1_END> timeInfo)){
  						
  						if(VastFilter.EXFIL_IPs_victimIpsList.contains(destinationIp)){
  							
  							return true;
  						}
  					}
  					
  					
  					
  				}
  				
  				
  				return false; 

  			
  			
  			}
  			});
    
    
    JavaRDD<String> filteredExfil_AllTraffic = words.filter(
            
        	new Function<String, Boolean>() {
      			public Boolean call(String word) { 
      				
      				String [] flowData = word.split("\\|");
      				
      				if(flowData.length>=14){
      					
      					if(flowData[0].trim().equals("")){
      						return false;
      					}
      					
      					
      					long timeInfo = Long.valueOf(flowData[0]);
      					String sourceIp = flowData[2];
      					String destinationIp = flowData[3];
      					
      					//check for Exfil
      					
      					if((VastFilter.EXFIL_timeWindow1_BEGIN < timeInfo)&&(VastFilter.EXFIL_timeWindow1_END> timeInfo)){
      						
      						if(VastFilter.EXFIL_IPs_victimIpsList.contains(destinationIp)){
      							
      							return true;
      						}
      						else{
      							return true;
      						}
      					}
      					
      					
      					
      				}
      				
      				
      				return false; 

      			
      			
      			}
      			});
    
    
//    
    JavaRDD<String> filteredDataDos = words.filter(
            
        	new Function<String, Boolean>() {
      			public Boolean call(String word) { 
      				
      				String [] flowData = word.split("\\|");
      				
      				if(flowData.length>=14){
      					
      					if(flowData[0].trim().equals("")){
      						return false;
      					}
      					
      					
      					long timeInfo = Long.valueOf(flowData[0]);
      					String sourceIp = flowData[2];
      					String destinationIp = flowData[3];
      					
      					//check for DoS
      					
      					if((VastFilter.DOS_timeWindow1_BEGIN < timeInfo)&&(VastFilter.DOS_timeWindow1_END > timeInfo)){
      						
      						if(VastFilter.DOS_IPs_List.contains(sourceIp) &&
      								(VastFilter.DOS_IPs_List.contains(destinationIp))){
      							
      							return true;
      						}
      					}

      					
      					
      					
      				}
      				
      				
      				return false; 

      			
      			
      			}
      			});
    
    JavaRDD<String> filteredBotNet = words.filter(
            
        	new Function<String, Boolean>() {
      			public Boolean call(String word) { 
      				
      				String [] flowData = word.split("\\|");
      				
      				if(flowData.length>=14){
      					
      					if(flowData[0].trim().equals("")){
      						return false;
      					}
      					
      					
      					long timeInfo = Long.valueOf(flowData[0]);
      					String sourceIp = flowData[2];
      					String destinationIp = flowData[3];
      					
      					//check for BotNet-DoS
      					if((VastFilter.Botnet_DOS_timeWindow1_BEGIN < timeInfo)&&(VastFilter.Botnet_DOS_timeWindow1_END > timeInfo)){
      						
      						if(VastFilter.Botnet_DOS_IPs_List.contains(sourceIp) &&
      								(VastFilter.Botnet_DOS_IPs_List.contains(destinationIp))){
      							
      							return true;
      						}
      					}
      					
      					
    
      					
      					
      					
      				}
      				
      				
      				return false; 

      			
      			
      			}
      			});
     
    
JavaRDD<String> filteredBotNet_AllData = words.filter(
            
        	new Function<String, Boolean>() {
      			public Boolean call(String word) { 
      				
      				String [] flowData = word.split("\\|");
      				
      				if(flowData.length>=14){
      					
      					if(flowData[0].trim().equals("")){
      						return false;
      					}
      					
      					
      					long timeInfo = Long.valueOf(flowData[0]);
      					String sourceIp = flowData[2];
      					String destinationIp = flowData[3];
      					
      					//check for BotNet-DoS
      					if((VastFilter.Botnet_DOS_timeWindow1_BEGIN < timeInfo)&&(VastFilter.Botnet_DOS_timeWindow1_END > timeInfo)){
      						
      						if(VastFilter.Botnet_DOS_IPs_List.contains(sourceIp) &&
      								(VastFilter.Botnet_DOS_IPs_List.contains(destinationIp))){
      							
      							return true;
      						}
      						else{
      							return true;
      						}
      					}
      					
      				}
      				
      				
      				return false; 

      			
      			
      			}
      			});
    
    
 // filteredDataDos.count();
  
  

    
    
    List<String> dos = filteredDataDos.collect();
  
  
  
  
  for(String i:dos){
	  System.out.println(i);
	  outDos.write(i.split("\\|")[2] +","+ i.split("\\|")[3]+"\n");
	  outDos.flush();
  }
  
 List<String> exfil = filteredExfil.collect();
  
  for(String i:exfil){
	  System.out.println(i);
	  
	  String key = i.split("\\|")[2] +","+ i.split("\\|")[3];
	  
	  if(VastFilter.EXFIL_FLOW_INFO.containsKey(key)){
		  
		  Long oldVal[] = VastFilter.EXFIL_FLOW_INFO.remove(key);
		  
		  //update Value
		  
		  oldVal[0] = oldVal[0] + Long.valueOf(i.split("\\|")[9]);
		  oldVal[1] = oldVal[1]+ Long.valueOf(i.split("\\|")[10]);
		  VastFilter.EXFIL_FLOW_INFO.put(key, oldVal);
		  
	  }
	  else{
		  Long[] val = new Long[2];
		  
		  val [0] = Long.valueOf(i.split("\\|")[9]);
		  val [1] = Long.valueOf(i.split("\\|")[10]);
		  VastFilter.EXFIL_FLOW_INFO.put(key, val);
	  }
	  
	  
	  
	  outExfil.write(i.split("\\|")[2] +","+ i.split("\\|")[3]+"\n");
	  outExfil.flush();
  }
  
 List<String> botNet = filteredBotNet.collect();
  
  for(String i:botNet){
	//  System.out.println(i);
	  outBotNet.write(i.split("\\|")[2] +","+ i.split("\\|")[3]+"\n");
	  outBotNet.flush();
  }
  
//  filteredDataDos.saveAsTextFile("Dos.csv");
//  filteredBotNet.saveAsTextFile("Botnet.csv");
//  filteredExfil.saveAsTextFile("exfil.csv");
  
  
for (String key : VastFilter.EXFIL_FLOW_INFO.keySet()){
	
	outExfil_flows.write(key + "," + VastFilter.EXFIL_FLOW_INFO.get(key)[0]+", "+VastFilter.EXFIL_FLOW_INFO.get(key)[1]+"\n");
	
	outExfil_flows.flush();
}
    
    

List<String> filteredExfil_AllTraffic_Collection = filteredExfil_AllTraffic.collect();




for(String i:filteredExfil_AllTraffic_Collection){
//	  System.out.println(i);
	  
	 boolean isAttackData = false;
	 
	 if(VastFilter.EXFIL_IPs_victimIpsList.contains(i.split("\\|")[3])){
		 isAttackData = true;
	 }
	  outExfil_AllTrafficDump.write(i.split("\\|")[2] +","+ i.split("\\|")[3]+","+isAttackData+"\n");
	  outExfil_AllTrafficDump.flush();
}



List<String> filteredBotNet_AllTraffic_Collection = filteredBotNet_AllData.collect();




for(String i:filteredBotNet_AllTraffic_Collection){
	//  System.out.println(i);
	  
		 boolean isAttackData = false;
		 
		 if((VastFilter.Botnet_DOS_IPs_List.contains(i.split("\\|")[3]))
				 
			&&	(VastFilter.Botnet_DOS_IPs_List.contains(i.split("\\|")[4])) ){
			 isAttackData = true;
		 }
	  
	  outBotNet_AllTrafficDump.write(i.split("\\|")[2] +","+ i.split("\\|")[3]+","+isAttackData+"\n");
	  outBotNet_AllTrafficDump.flush();
}




   
   if( ctx.statusTracker().getActiveJobIds().length==0){
	   outDos.close();
	   outBotNet.close();
	   outExfil.close();
	   outExfil_flows.close();
	   outBotNet_AllTrafficDump.close();
	   outExfil_AllTrafficDump.close();
	   
	   ctx.stop();
   }
   else{
	   System.out.println("Active Jobs  :"+ctx.statusTracker().getActiveJobIds().length);
   }
  }
}
