package MapReduce;

import java.io.*;

public static void cleansing(String inputfile) throws IOException {
    	passengerDataFile = new ArrayList<>();
    	
    	BufferedReader br = new BufferedReader(new FileReader(inputfile)); //reader for input file intitialized only once
	    String strLine = null;
    	
	    while ((strLine = br.readLine()) != null) {
	    	String temp[] = strLine.split(",");
            if ( temp[0].isEmpty() )// if row is empty
            {
            	System.out.println("ERROR : Empty Records ");
            }
            else if (temp[1].matches("^[A-Z]{3}\\d{4}\\[A-Z]{2}\\d{1}") == false ) {
            	System.out.println("ERROR Passenger ID:");
            }
            else if (temp[2].matches("^[A-Z]{3}\\d{4}\\[A-Z]{1}") == false ) {
            	System.out.println("ERROR Flight ID");
            }
            else if (temp[3].matches("^[A-Z]{3}") == false ) {
            	System.out.println("From airport IATA/FAA code:");
            }
            else if (temp[1].matches("^[A-Z]{3}") == false ) {
            	System.out.println("Destination airport IATA/FAA code:");
            }
            else {
	    	      passengerDataFile.add(strLine);
            }
	    }    
	    br.close(); 	
  	}

public class MapReduce {public static void main(int args[], String argstr[])  
	{ 
		try{  	        
			
      // variable intialization
      passengerFile = argstr[0]; //"C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
			airportFile = argstr[1]; //"C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
			
      // Step 1 : Cleansing / Preprocessing
	       cleansing(passengerFile);
		  // Step 2: split files into multiple files or parallel processing
		  // Step 3: Mapper 
      // Step 4: Reducer
		  }
			 catch (IOException e) {
			  e.printStackTrace();
			}
	}
