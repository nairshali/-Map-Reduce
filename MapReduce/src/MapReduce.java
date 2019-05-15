package MapReduce;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

//
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
// Time calculation
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MapReduce {
    
    public static  String passengerFile = null;
    public static  String airportFile = null;
    public static  int jobId ;
    public static  String flightId;
    // file Map
    public static  Map<String,String> mapAirport = null;
    public static  Map<String,String> mapAirportNames = null;
    public static  Map<String,Integer> mapAirportIATA = null;
    // cleaning
    public static List<String> passengerDataFile = new ArrayList<>();
    // file split
    public static List<String> dataFile = new ArrayList<>();
    public static List<String>[] arrayOfdataFile;
    // Map data file
    public static List<String> mapDataFile = new ArrayList<>();
    public static List<String>[] arrayOfmapDataFile;
    public static  HashMap<String, Integer> map1 = new HashMap<String,Integer>();
    public static  HashMap<String, Integer> map2 = new HashMap<String,Integer>();
    
    public static void readFileandMap(int fileNo ) throws Exception {
        
	// read airport data    
	mapAirport = new HashMap<String,String>();
	mapAirportNames = new HashMap<String,String>();
	mapAirportIATA = new HashMap<String,Integer>();
	// Initialize
	mapDataFile = new ArrayList<>();
		
	FileInputStream fstream2 = new FileInputStream(airportFile);
        String strLine1;
        // Use DataInputStream to read binary NOT text.
        BufferedReader br1 = new BufferedReader(new InputStreamReader(fstream2));
        while ((strLine1 = br1.readLine()) != null) {
            String temp[] = strLine1.split(",");
            mapAirport.put( temp[1],temp[2]+"," +temp[3]);
            mapAirportNames.put(temp[1],temp[0]);
            mapAirportIATA.put(temp[1],0);
        }
        
        br1.close();
	
	// read passenger data    
        List<String> list = arrayOfdataFile[fileNo];
        map1 = new HashMap<String,Integer>();
        
	for (int i = 0; i < list.size(); i++) {
        	
            String temp[] = list.get(i).split(",");
		
            if (jobId == 0) {
                map1.put( temp[2]+"-"+temp[1], 1);
            }
            else if (jobId == 1) {
                if ( temp[1].compareTo(flightId) == 0 ) {
                	int seconds = Integer.parseInt(temp[4]);
    	            	int timesAdd = Integer.parseInt(temp[5]);
    	            	    	            		
    	            	LocalDateTime dateTime = LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC);
    	            	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ENGLISH);
    	            	String depatureTime = dateTime.format(formatter);
    	            	//System.out.println(formattedDate); 
    	            		
    	            	LocalTime time = LocalTime.parse(depatureTime);
    	            	String arrivalTime = formatter.format(time.plusMinutes(timesAdd));
    	            	
    	            	DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("HH:mm", Locale.ENGLISH);
    	            	String flightTime = formatter1.format(time.plusMinutes(timesAdd));
    	            	//String flightTime = formatter.format(time1);
    	            	System.out.println(flightTime);
    	            	
                	map1.put( temp[0]+"-"+temp[2]+"-"+ temp[3]+"-"+depatureTime+"-"+arrivalTime+"-"+flightTime, 1);
                }
            }
            else if (jobId == 2) {
               	map1.put( temp[1]+"-"+temp[0], 1);
            }
            else if (jobId == 3) {
                String temp1[] = mapAirport.get(temp[2]).split(","); 
                
		// Nautical Miles
                double lat1 = Double.parseDouble(temp1[0]);
                double log1 = Double.parseDouble(temp1[1]);
                String temp2[] = mapAirport.get(temp[3]).split(",");
                double lat2 = Double.parseDouble(temp2[0]);
                double log2 = Double.parseDouble(temp2[1]);
                double nauticalmiles = distance(lat1, log1, lat2, log2);
                	
                try {
                	String strNautical = temp[1] +"-"+ temp[0] +"-"+ Math.round(nauticalmiles*100)/100;
                	if ( map1.get(strNautical)  == null ) {
                		map1.put( strNautical, 1);
			}
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
            }          
       }
 
       for (String i : map1.keySet()) {
	   mapDataFile.add(i + "," + map1.get(i));
        }

        System.out.println("mapDataFile"+ mapDataFile.size());
        arrayOfmapDataFile[fileNo] = mapDataFile;
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
	int count = 0; 			
	int threadNum = args[1]; // No Of Parallel Processing
        passengerFile = argstr[0]; //"C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
        airportFile = argstr[1]; //"C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
		
	MapReduce.jobId = args[0]; // Job No    
	MapReduce.flightId = argstr[2];  // Flight ID
		 
	// Parallel Processing
	ExecutorService executor = Executors.newFixedThreadPool(threadNum);
	List<FutureTask<Integer>> taskList = new ArrayList<FutureTask<Integer>>();
	
	try{  	        
	    	
            // Step 1 : Cleansing / Preprocessing
	     cleansing(passengerFile);
	    // Step 2: Split files into multiple files or parallel processing
	     FileSplit(threadNum);
	    // Step 3: Mapper 
             try {
		for (int i=0;i<threadNum;i++) { 	
			int fileNo = i; // files
			System.out.println("fileNo"+fileNo);
			
			// Parallel Execution
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
		 
            // Step 4: Reducer
	}
	catch (IOException e) 
	{
	   e.printStackTrace();
	}
    }
}
