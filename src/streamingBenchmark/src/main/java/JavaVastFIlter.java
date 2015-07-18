import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

//import org.apache.spark.examples.VastFilter;


public class JavaVastFIlter {
	
	static long DOS_timeWindow1_BEGIN = 1364860800;
	static long DOS_timeWindow1_END = 1365030000;
	  
	static String [] DOS_IPs = {"10.98.107.5", "172.30.0.04", "172.20.0.4"};
	static ArrayList<String> DOS_IPs_List = new ArrayList<String>( Arrays.asList(DOS_IPs));
	 
	static long Botnet_DOS_timeWindow1_BEGIN = 1365811200;
	static long Botnet_DOS_timeWindow1_END = 1365980400;
	  
	static String [] Botnet_DOS_IPs = {"10.0.0.8", "172.20.1.47", "172.10.2.135", "172.20.1.81", "172.10.2.66", "172.30.1.223"};
	static ArrayList<String> Botnet_DOS_IPs_List = new ArrayList<String>( Arrays.asList(Botnet_DOS_IPs));
	  
	static long EXFIL_timeWindow1_BEGIN = 1365206400;
	static long EXFIL_timeWindow1_END =  1365375600;
	  
	static String [] EXFIL_IPs_victimIps = {"10.7.5.5"};
	static ArrayList<String> EXFIL_IPs_victimIpsList = new ArrayList<String>( Arrays.asList(EXFIL_IPs_victimIps));
	public static void main(String []args){
		
		
		String filteredData = "";
		try {
		    BufferedReader in = new BufferedReader(new FileReader("/home/hduser/vast_netflow_100.txt"));
		    String str;
		    while ((str = in.readLine()) != null) {
		        
		    	String [] pieces = str.split("\\|");
		    	
		    	if(filter(pieces)==0){
		    		
		    		filteredData  = filteredData +"DOS," +str.replace("|", ",")+"\n"; 
		    	}
		    	else if(filter(pieces)==1){
		    		filteredData  = filteredData +"BotNet," +str.replace("|", ",")+"\n"; 
		    		
		    	}
		    	else if(filter(pieces)==2){
		    		filteredData  = filteredData +"Exfil," +str.replace("|", ",")+"\n"; 
		    		
		    	}		   
		    	
		    }
		    in.close();
		} catch (IOException e) {
		}

		
		try {
		    BufferedWriter out = new BufferedWriter(new FileWriter("vast_netflow_filter.txt"));
		    out.write(filteredData);
		    out.flush();
		    out.close();
		} catch (IOException e) {
		}

		
		
	}
	
	public static int filter(String [] flowData){
		if(flowData.length>=14){
			if(flowData[0].trim().equals("")){
				return -1;
				}	
				long timeInfo = Long.valueOf(flowData[0]);
				String sourceIp = flowData[2];
				String destinationIp = flowData[3];
				
				//check for DoS
				
				if((JavaVastFIlter.DOS_timeWindow1_BEGIN < timeInfo)&&(JavaVastFIlter.DOS_timeWindow1_END > timeInfo)){
					
					if(JavaVastFIlter.DOS_IPs_List.contains(sourceIp) &&
							(JavaVastFIlter.Botnet_DOS_IPs_List.contains(destinationIp))){
						
						return 0;
					}
				}
				
				//check for BotNet-DoS
				if((JavaVastFIlter.Botnet_DOS_timeWindow1_BEGIN < timeInfo)&&(JavaVastFIlter.Botnet_DOS_timeWindow1_END > timeInfo)){
					
					if(JavaVastFIlter.Botnet_DOS_IPs_List.contains(sourceIp) &&
							(JavaVastFIlter.Botnet_DOS_IPs_List.contains(destinationIp))){
						
						return 1;
					}
				}
				
				
				//check for Exfil
				
				if((JavaVastFIlter.EXFIL_timeWindow1_BEGIN < timeInfo)&&(JavaVastFIlter.EXFIL_timeWindow1_END> timeInfo)){
					
					if(JavaVastFIlter.EXFIL_IPs_victimIpsList.contains(destinationIp)){
						
						return 2;
					}
				}
				
				
				
			}
			
			
			return 0; 

		
		
		}
		
	
	

}

