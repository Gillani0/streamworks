package gov.pnnl.datasciences.sparkstreaming;


import java.io.File;


/** Parses the command line arguments.
 * 
 * @author Patrick Mackey
 * @author Vivek Datla
 */

/*
 * Vivek Datla Notes: I reused the code for spark streaming. Modified the code for spark streaming requirements.
 * */
public class CmdLineArgs
{
	String _fileName = null;
	int _numRecordsFile = 1;
	int _numRecordsPerMessage= 1;
	TrafficFileType _fileType = null;
	int _durSecs = 30; // Default duration: 30 seconds
	int _delayMillisecs = 0; // Default delay: 0 seconds (no delay)
	int _numWorkers = 1; // Default number of workers
	boolean _success = false; // Stores whether or not we could successfully parse command line arguments
	boolean _verbose = false; // Whether or not to display extra output for testing
	boolean _webOnly = false; // Whether or not we filter out non-web traffic
	boolean _publicOnly = false; // Whether or not we filter out private traffic
	String streamingIP = null;
	String streamingPort = null;
//	boolean _preload = false; // Whether or not to preload data into memory before emitting it
	
	/**
	 * Constructor parses the command line arguments.
	 * @param vars  The command line args received from the Main function.
	 */
	public CmdLineArgs(String [] vars)
	{
		if(vars.length < 1)
		{
			dispHelp();
			return;
		}
		
		_fileName = vars[0];
		if(doesFileExist(_fileName)==false)
		{
			System.out.println("The file '" + _fileName + "' was unable to be open and read.");
			dispHelp();
			return;
		}
		
		for(int i=1; i<vars.length; i++)
		{
			String var = vars[i];
			// This is only used for benchmarking, does'nt determine the number of records processed
			if(var.equals("-size"))
			{
				i++;
				if(i >= vars.length)
				{
					System.out.println("You must specify a number of records after the argument -size");
					dispHelp();
					return;
				}
				try
				{
					_numRecordsFile = Integer.parseInt(vars[i]);
				}
				catch(Exception e)
				{
					System.out.println("Unable to parse number of records in file" + e.getMessage());
					dispHelp();
					return;
				}
			}
			else if(var.equals("-numRecords"))
			{
				i++;
				if(i >= vars.length)
				{
					System.out.println("You must specify a number of records after the argument -size");
					dispHelp();
					return;
				}
				try
				{
					_numRecordsPerMessage = Integer.parseInt(vars[i]);
				}
				catch(Exception e)
				{
					System.out.println("Unable to parse number of records in file" + e.getMessage());
					dispHelp();
					return;
				}
			}
			else if(var.equals("-netflow"))
			{
				if(_fileType == TrafficFileType.CAIDA)
				{
					System.out.println("You cannot assign a file to be both -netflow and -caida");
					dispHelp();
					return;
				}
				_fileType = TrafficFileType.NETFLOW;
			}
			else if(var.equals("-caida"))
			{
				if(_fileType == TrafficFileType.NETFLOW)
				{
					System.out.println("You cannot assign a file to be both -netflow and -caida");
					dispHelp();
					return;
				}
				_fileType = TrafficFileType.CAIDA;
			}
			else if(var.equals("-workers"))
			{
				i++;
				if(i >= vars.length)
				{
					System.out.println("You must specify a number of workers after the argument -workers");
					dispHelp();
					return;
				}
				try
				{
					_numWorkers = Integer.parseInt(vars[i]);
				}
				catch(Exception e)
				{
					System.out.println("Unable to parse number of workers. " + e.getMessage());
					dispHelp();
					return;
				}
			}
			else if(var.equals("-time"))
			{
				i++;
				if(i >= vars.length)
				{
					System.out.println("You must specify a number of seconds after the argument -time");
					dispHelp();
					return;
				}
				try
				{
					_durSecs = Integer.parseInt(vars[i]);
				}
				catch(Exception e)
				{
					System.out.println("Unable to parse number of seconds. " + e.getMessage());
					dispHelp();
					return;
				}
			}
			else if(var.equals("-delay"))
			{
				i++;
				if(i >= vars.length)
				{
					System.out.println("You must specify a number of milliseconds after the argument -delay");
					dispHelp();
					return;
				}
				try
				{
					_delayMillisecs = Integer.parseInt(vars[i]);
				}
				catch(Exception e)
				{
					System.out.println("Unable to parse number of milliseconds. " + e.getMessage());
					dispHelp();
					return;
				}
			}
			else if(var.equals("-verbose"))
			{
				_verbose = true;
			}
			else if(var.equals("-web"))
			{
				_webOnly = true;
			}
			else if(var.equals("-public"))
			{
				_publicOnly = true;
			}
//			else if(var.equals("-agg"))
//			{
//				_edgeAgg = EdgeAggregation.IP_And_Port;
//			}
//			else if(var.equals("-agg2"))
//			{
//				_edgeAgg = EdgeAggregation.IP_Only;
//			}
//			else if(var.equals("-preload"))
//			{
//				_preload = true;
//			}
			else
			{
				System.out.println("Unknown command line argument: '" + var + "'");
				dispHelp();
				return;
			}
		}
		
		if(_fileType == null)
		{
			System.out.println("You must specify whether the file is a netflow or caida file: -netflow or -caida");
			dispHelp();
			return;
		}
		
		// If we pass all the tests, set success to be true
		_success = true;
	}
	
	/**
	 * Displays help to the command line describing what arguments are acceptable.
	 */
	private void dispHelp()
	{
		System.out.println("To use: ");
		System.out.println("SparkStreaming.jar [netflow_file] [options]");
		System.out.println("Options:");
		System.out.println(" -netflow = Netflow file");
		System.out.println(" -caida = Caida file");
		System.out.println(" -size [num_records_in_file] = Number of records/lines in input file(used for benchmarking only)");
		System.out.println(" -numRecords [num_records_per_message] = Number of records to send per activemq message");
		System.out.println(" -workers [num_workers] = Number of workers for executing the task on each slave");
		System.out.println(" -time [seconds] = Duration to exceute (in seconds)");
		System.out.println(" -delay [milliseconds] = Duration to wait between sending traffic records");
		System.out.println(" -verbose = Extra output for testing purposes");
		System.out.println(" -web = Filter out all non-web traffic");
		System.out.println(" -public = Filter out private traffic");
		System.out.println(" -agg = Aggregate edges based on IP address and port");
		System.out.println(" -agg2 = Aggregate edges based on IP address only");
		System.out.println(" -preload = Preload data file into memory before emitting it");
		System.out.println(" -streamingIp = IP address of the streaming content");
		System.out.println(" -streamingPort = Port of the streaming content");
		
	}
		
	private boolean doesFileExist(String fname)
	{
		File test = new File(_fileName);
		return test.exists() && test.canRead();			
	}
	
	/** @return True if the arguments were able to be parsed successfully */
	public boolean isSuccessful()
	{
		return _success;
	}
	
	/** @return The input filename containing the data */
	public String getFileName()
	{
		return _fileName;
	}
	/** @return The file type (Netflow or Caida) */
	public TrafficFileType getFileType()
	{
		return _fileType;
	}
	/** @return The amount of time to continue the process */
	public int getDurationSecs()
	{
		return _durSecs;
	}
	/** @return The amount of time to delay between sending a new row of data */
	public int getDelayMillisecs()
	{
		return _delayMillisecs;
	}
	/** @return The number of threads to use for each bolt */
	public int getNumThreads()
	{
		return _numWorkers;
	}
	/** @return The number of records inf ile used for benchmarking */
	public int getNumRecords()
	{
		return _numRecordsFile;
	}
	public int getNumRecordsPerMessage()
	{
		return _numRecordsPerMessage;
	}
	/** @return If true, reports additional information to the command line */
	public boolean isVerbose()
	{
		return _verbose;
	}
	/** @return If true, filter out all non-web traffic */
	public boolean isWebOnly()
	{
		return _webOnly;
	}
	/** @return If true, filter out all private traffic */
	public boolean isPublicOnly()
	{
		return _publicOnly;
	}
//	/** @return The type of edge aggregation being used */
//	public EdgeAggregation getEdgeAggregation()
//	{
//		return _edgeAgg;
//	}
//	/** @return If true, loads all of the data from the file into memory before it starts to send it through the Spout */
//	public boolean isPreload()
//	{
//		return _preload;
//	}
}