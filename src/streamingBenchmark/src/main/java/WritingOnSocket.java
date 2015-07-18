import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class WritingOnSocket {

	public static void main(String[] args) throws InterruptedException {

		Socket smtpSocket = null;
		DataOutputStream os = null;

		try {
			 smtpSocket = new Socket("192.168.0.1", 9999);
		 os = new DataOutputStream(smtpSocket.getOutputStream());
		
			

		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: hostname");
		} catch (IOException e) {
			System.err
					.println("Couldn't get I/O for the connection to: hostname");
		}
		// If everything has been initialized then we want to write some data
		// to the socket we have opened a connection to on port 25
//		if (smtpSocket != null) {
			try {
				// The capital string before each colon has a special meaning to
				// SMTP
				// you may want to read the SMTP specification, RFC1822/3

				BufferedReader in = new BufferedReader(
						new FileReader(
								"/home/hduser/spark_scratchPad/equinox-sanjose.20120119-netflow.txt"));
				String str;
				while ((str = in.readLine()) != null) {
					
					String [] pieces = str.split(" +");
					String cleanStr="";
					for(String piece:pieces){
						cleanStr = cleanStr+"\t"+piece;
						cleanStr = cleanStr.trim();
					}
//					System.out.print(cleanStr+"\n");
//					os = new DataOutputStream(smtpSocket.getOutputStream());
					os.writeChars(cleanStr + "\n");
				
					os.flush();
//					os.close();
//					if(!pieces[2].trim().equalsIgnoreCase("start")){
//					int sleepTime = Math.round(Float.parseFloat(pieces[2])*1000);
//					TimeUnit.MILLISECONDS.sleep(sleepTime);
//					}
					
				}

				
				smtpSocket.close();

				// clean up:
				// close the output stream
				// close the input stream
				// close the socket
				

				
			} catch (UnknownHostException e) {
				System.err.println("Trying to connect to unknown host: " + e);
			} catch (IOException e) {
				System.err.println("IOException:  " + e);
			}catch(NumberFormatException e){
				System.err.println("NumberFormatException:  " + e);
			}
//		}
	}

}
