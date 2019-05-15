package MapReduce;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

//// 
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class MapReduce {
	
    // cleaning
    public static List<String> passengerDataFile = new ArrayList<>();
    // file split
    public static List<String> dataFile = new ArrayList<>();
    public static List<String>[] arrayOfdataFile;
    // Map data file
    public static List<String> mapDataFile = new ArrayList<>();
    public static List<String>[] arrayOfmapDataFile;
    
    public static void readFileandMap(String filename, String resultfile, int fileNo ) throws Exception {
    }

    public static void FileSplit(int files) throws FileNotFoundException, IOException{

	int lines = 0;  //set this to whatever number of lines you need in each file
	int count = passengerDataFile.size();
	int i = 0;
	    
	// file split
	arrayOfdataFile = new List[files];
	// file map
	arrayOfmapDataFile = new List[files];
	
	if((count%files)==0){  
	      lines = (count/files);  
	 }  
	 else{  
	      lines=( (count+1) /files);  
	 }  
	     
	 System.out.println(files); //number of files that shall eb created
	 System.out.println(lines);
	 
	for (int j = 0; j < passengerDataFile.size(); j++) 
	{
	   if( (j%lines) == 0 ){ 
	      System.out.println("dataFile" + dataFile.size());	   
	      arrayOfdataFile[i] = dataFile;
	      dataFile  = new ArrayList<>();
	      i++;	   
	   }
	   strLine = passengerDataFile.get(j);   
	   dataFile.add(strLine);
	}
	System.out.println("arrayOfdataFile.length" + arrayOfdataFile.length);
   }

   public static void cleansing(String inputfile) throws IOException {
    	passengerDataFile = new ArrayList<>();
    	
    	BufferedReader br = new BufferedReader(new FileReader(inputfile)); //reader for input file intitialized only once
	String strLine = null;
    	
	// Error File
	FileWriter fstream1 = null;
	try {
		fstream1 = new FileWriter(inputfile.substring(0, inputfile.lastIndexOf("\\")+1)+ "ERRORFILE.txt");
	} catch (IOException e) {
	// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
        BufferedWriter out = new BufferedWriter(fstream1); 
	    while ((strLine = br.readLine()) != null) {
	    	String temp[] = strLine.split(",");
		 if ( temp[0].isEmpty() ){
		     System.out.println("ERROR : Empty Records ");
		     out.write("ERROR : Empty Records");
	             out.newLine();
		  }
		  else if (temp[0].matches("^[A-Z]{3}\\d{4}[A-Z]{2}\\d{1}") == false ) {
		     System.out.println("ERROR Passenger ID:" + temp[0]);
		     out.write("ERROR Passenger ID:" + temp[0]);
		     out.newLine();  
		  }
		  else if (temp[1].matches("^[A-Z]{3}\\d{4}[A-Z]{1}") == false ) {
		     System.out.println("ERROR Flight ID" + temp[1]);
		     out.write("ERROR Flight ID" + temp[1]);
	             out.newLine();	  
		  }
		  else if (temp[2].matches("^[A-Z]{3}") == false ) {
		     System.out.println("From airport IATA/FAA code:" + temp[2]);
		     out.write("From airport IATA/FAA code:" + temp[2]);
		     out.newLine();	  
		  }
		  else if (temp[3].matches("^[A-Z]{3}") == false ) {
		     System.out.println("Destination airport IATA/FAA code:" + temp[3]);
		     out.write("Destination airport IATA/FAA code:" + temp[3]);
	             out.newLine();	  
		  }
		  else {
		     passengerDataFile.add(strLine);
		  }
	    }    
	    br.close(); 
	    out.close();
  	}

   public static void main(int args[], String argstr[]) { 
	
	// variable intialization
	int threadNum = args[1]; // No Of Parallel Processing
        passengerFile = argstr[0]; //"C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
        airportFile = argstr[1]; //"C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
		 
	// Parallel Processing
	ExecutorService executor = Executors.newFixedThreadPool(threadNum);
	List<FutureTask<Integer>> taskList = new ArrayList<FutureTask<Integer>>();
	
	try{  	        
	    	
            // Step 1 : Cleansing / Preprocessing
	     cleansing(passengerFile);
	    // split files into multiple files or parallel processing
	     FileSplit(threadNum);
	    
	    // Parallel Execution
             try {
		for (int i=0;i<threadNum;i++) { 	
			int fileNo = i; // files
			System.out.println("fileNo"+fileNo);
			
			// parallel
			FutureTask<Integer> Task = new FutureTask<Integer>(new Callable<Integer>() {
				@Override
				public Integer call() {
					try {
					     MapReduce.readFileandMap(fileNo);
					} 
					catch (Exception e) {
					     // TODO Auto-generated catch block
					     e.printStackTrace();
					}
					return 1;	
				}
			});
						     
			taskList.add(Task);
			executor.execute(Task);
		}
		     
	    // Step 3: Mapper 
            // Step 4: Reducer
	}
	catch (IOException e) 
	{
	   e.printStackTrace();
	}
    }
}
