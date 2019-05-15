package MapReduce;

import java.io.*;

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
