
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class Cat{
        public static void main (String [] args) throws Exception{
                try{
                        Path pt=new Path("hdfs://192.168.0.1:9000/user/hduser/equinox-sanjose.20120119-netflow.txt");
                        
                        Configuration conf = new Configuration();
                        conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
                        conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
//                        FileSystem fs = FileSystem.get(conf);
                        FileSystem fs = pt.getFileSystem(conf);
                        System.out.println(fs.getHomeDirectory());
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                        String line;
                        line=br.readLine();
                        while (line != null){
                                System.out.println(line);
                                line=br.readLine();
                        }
                }catch(Exception e){
                	System.out.println (e);
                }
        }
}