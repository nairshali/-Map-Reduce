package MapReduce;
/*
 MapReduce class for actual execution of Task's. Its contains Mapper and Reducer user programs.
 Cleansing : Filter and clean data, validate data format and cross reference airport file for existence of Airport
 FileSplit : Split file as per no of threads
 readFileandMap : read input files and create key value pairs
 combine : combine data provided by readFileandMap user programs
 sortshuffle : Sort key value pair in asc order, shuffle and partition data as per no of thread
 reduce :  Merge Output from sortshuffle phase
*/

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
//
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
// Time calculation
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MapReduce {

	// variable for file name
	public static String passengerFile = null;
	public static String airportFile = null;
	public static int jobId;
	public static String flightId;

	// file Map
	public static Map<String, String> mapAirport = null;
	public static Map<String, String> mapAirportNames = null;
	public static Map<String, Integer> mapAirportIATA = null;

	// cleansing
	public static List<String> passengerDataFile = new ArrayList<>();

	// file split
	public static List<String> dataFile = new ArrayList<>();
	public static List<String>[] arrayOfdataFile;

	// Map data file
	public static List<String> mapDataFile = new ArrayList<>();
	public static List<String>[] arrayOfmapDataFile;

	// Combine data file
	public static List<String> combineDataFile = new ArrayList<>();
	public static List<String>[] arrayOfCombineDataFile;

	// interim results
	public static List<String> interimDataFile = new ArrayList<>();

	// SortShuffleDataFile
	public static List<String> sortShuffleDataFile = new ArrayList<>();
	public static List<String>[] arrayOfsortShuffleDataFile;

	// initialize map data structure
	public static HashMap<String, Integer> map1 = new HashMap<String, Integer>();
	public static HashMap<String, Integer> map2 = new HashMap<String, Integer>();

	/* :: This function calculate distance between two airport : */
	public static double distance(double lat1, double lon1, double lat2, double lon2) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515 * 0.8684;
		return (dist);
	}

	/* :: This function converts decimal degrees to radians : */
	public static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/* :: This function converts radians to decimal degrees : */
	public static double rad2deg(double rad) {
		return (rad * 180.0 / Math.PI);
	}

	/* :: Merge output from previous sort and shuffle phase : */
	public static void reduce(TreeMap<String, Integer> argsmap1, TreeMap<String, Double> argsmap2[]) throws Exception {

		map2 = new HashMap<String, Integer>();

		for (int i = 0; i < arrayOfsortShuffleDataFile.length; i++) {
			List<String> list = arrayOfsortShuffleDataFile[i];

			for (int j = 0; j < list.size(); j++) {
				String strLine = list.get(j);
				if (strLine != null) {
					String temp[] = strLine.split(",");
					map2.put(temp[0], 1);
				}
			}
		}

		// Objective 1
		if (jobId == 0) {

			for (String i : map2.keySet()) {
				String temp[] = i.split("-");
				if (mapAirportIATA.get(temp[0]) != null) {
					mapAirportIATA.put(temp[0], mapAirportIATA.get(temp[0]) + 1);
				} else {
					mapAirportIATA.put(temp[0], 1);
				}
			}

			for (String i : mapAirportIATA.keySet()) {
				argsmap1.put(mapAirportNames.get(i), mapAirportIATA.get(i));
			}

		}
		// Objective 2
		else if (jobId == 1) {

			argsmap1.putAll(map2);
		}
		// Objective 3
		else if (jobId == 2) {
			for (String i : map2.keySet()) {
				String temp[] = i.split("-");
				if (argsmap1.get(temp[0]) != null) {
					argsmap1.put(temp[0], argsmap1.get(temp[0]) + 1);
				} else {
					argsmap1.put(temp[0], 1);
				}
			}
		}
		// Objective 4
		else if (jobId == 3) {

			for (String i : map2.keySet()) {
				String temp[] = i.split("-");
				double naut = Double.parseDouble(temp[2]);

				// Flight Nautical
				argsmap2[0].put(temp[0], naut);

				// Traveller Nautical
				if (argsmap2[1].get(temp[1]) == null) {
					argsmap2[1].put(temp[1], naut);
				} else {
					argsmap2[1].put(temp[1], argsmap2[1].get(temp[1]) + naut);
				}

			}

			// Highest Traveller Nautical
			Double maxValueInMap = (Collections.max(argsmap2[1].values()));
			for (String i1 : argsmap2[1].keySet()) {
				if (argsmap2[1].get(i1) == maxValueInMap) {
					argsmap2[2].put(i1, maxValueInMap);
				}
			}

		}

	}

	/*
	 * :: Sort key value pair in asc order, shuffle and partition data as per no of
	 * thread :
	 */
	public static void sortShuffle(int files) throws Exception {

		// Initialize
		interimDataFile = new ArrayList<>();
		sortShuffleDataFile = new ArrayList<>();
		String tempKey = null;

		for (int i = 0; i < arrayOfCombineDataFile.length; i++) {
			List<String> list = arrayOfCombineDataFile[i];
			interimDataFile.addAll(list);

		}

		// sort and Shuffle
		interimDataFile.sort(null);

		// for single shuffling not required
		if (arrayOfCombineDataFile.length == 1) {
			arrayOfsortShuffleDataFile[0] = interimDataFile;
		} else {
			// spilt data
			int count = interimDataFile.size() - 1;
			int lines = 0;
			int filecnt = 0;
			int partionCnt = 0;

			if ((count % files) == 0) {
				lines = (count / files);
			} else {
				lines = ((count + 1) / files);
			}

			for (int k = 0; k < interimDataFile.size(); k++) {
				String strLine = interimDataFile.get(k);
				String temp[] = strLine.split(",");

				if ((k % lines == 0) && (k != 0)) {
					partionCnt = 1;
				}

				// check if file need to split
				if (partionCnt == 1 && filecnt != files - 1) {
					// group same key in one partition
					if (temp[0].equals(tempKey) == false) {
						arrayOfsortShuffleDataFile[filecnt] = sortShuffleDataFile;
						sortShuffleDataFile = new ArrayList<>();
						sortShuffleDataFile.add(strLine);
						filecnt++;
						partionCnt = 0;
					}
					// stored in intermediate buffer
					else {
						sortShuffleDataFile.add(strLine);
					}
				} else {
					sortShuffleDataFile.add(strLine);
				}

				tempKey = temp[0];
			}

			// write final data set
			if (filecnt == files - 1) {
				arrayOfsortShuffleDataFile[files - 1] = sortShuffleDataFile;
			} else {
				// check if final data sets has been written to buffer
				if (arrayOfsortShuffleDataFile[files - 1] == null) {
					arrayOfsortShuffleDataFile[files - 1] = sortShuffleDataFile;
				}
			}
		}

	}

	/* :: combine data provided by readFileandMap user programs : */
	public static void combine(int fileNo) throws Exception {
		// Initialize
		combineDataFile = new ArrayList<>();
		HashMap<String, Integer> combinemap = new HashMap<String, Integer>();

		// read passenger data
		List<String> listdata = arrayOfmapDataFile[fileNo];

		// Combine Key,Value
		for (int i = 0; i < listdata.size(); i++) {
			String temp[] = listdata.get(i).split(",");
			combinemap.put(temp[0], Integer.parseInt(temp[1]));
		}

		// Save result into intermediate
		for (String i : combinemap.keySet()) {
			combineDataFile.add(i + "," + combinemap.get(i));
		}

		arrayOfCombineDataFile[fileNo] = combineDataFile;
	}

	/* :: read input files and create key value pairs, calculate Nautical miles : */
	public static void readFileandMap(int fileNo) throws Exception {

		// Initialize
		mapDataFile = new ArrayList<>();

		// read passenger data
		List<String> list = arrayOfdataFile[fileNo];
		map1 = new HashMap<String, Integer>();

		for (int i = 0; i < list.size(); i++) {

			String temp[] = list.get(i).split(",");

			// objective 1
			if (jobId == 0) {
				mapDataFile.add(temp[2] + "-" + temp[1] + "," + 1);
			}
			// objective 2
			else if (jobId == 1) {
				if (temp[1].compareTo(flightId) == 0) {
					int seconds = Integer.parseInt(temp[4]);
					int timesAdd = Integer.parseInt(temp[5]);

					LocalDateTime dateTime = LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ENGLISH);
					String depatureTime = dateTime.format(formatter);

					LocalTime time = LocalTime.parse(depatureTime);
					String arrivalTime = formatter.format(time.plusMinutes(timesAdd));

					DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("HH:mm", Locale.ENGLISH);
					String flightTime = formatter1.format(time.plusMinutes(timesAdd));
					// String flightTime = formatter.format(time1);

					mapDataFile.add(temp[0] + "-" + temp[2] + "-" + temp[3] + "-" + depatureTime + "-" + arrivalTime
							+ "-" + flightTime + "," + 1);
				}
			}
			// objective 3
			else if (jobId == 2) {
				mapDataFile.add(temp[1] + "-" + temp[0] + "," + 1);
			}
			// objective 4
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
					String strNautical = temp[1] + "-" + temp[0] + "-" + Math.round(nauticalmiles * 100) / 100;
					if (map1.get(strNautical) == null) {
						mapDataFile.add(strNautical + "," + 1);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		arrayOfmapDataFile[fileNo] = mapDataFile;
	}

	/* :: Split file as per no of threads : */
	public static void FileSplit(int files) throws FileNotFoundException, IOException {

		int lines = 0; // set this to whatever number of lines you need in each file
		int count = passengerDataFile.size() - 1;
		int i = 0;

		// file split
		arrayOfdataFile = new List[files];
		// file map
		arrayOfmapDataFile = new List[files];
		// combiner
		arrayOfCombineDataFile = new List[files];
		// sort shuffle
		arrayOfsortShuffleDataFile = new List[files];

		if ((count % files) == 0) {
			lines = (count / files);
		} else {
			lines = ((count + 1) / files);
		}

		for (int j = 0; j < passengerDataFile.size(); j++) {
			String strLine = passengerDataFile.get(j);
			dataFile.add(strLine);

			if ((j % lines == 0) && (j != 0)) {
				arrayOfdataFile[i] = dataFile;
				dataFile = new ArrayList<>();
				i++;
			}
		}
	}

	/*
	 * :: Filter and clean data, validate data format and cross reference airport
	 * file for existence of Airport :
	 */
	public static void cleansing(String inputfile) throws IOException {

		// Record No
		int recNo = 1;

		// Error File
		FileWriter fstream1 = null;
		try {
			fstream1 = new FileWriter(
					passengerFile.substring(0, passengerFile.lastIndexOf("\\") + 1) + "ERRORFILE.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BufferedWriter out = new BufferedWriter(fstream1);

		// read airport data
		mapAirport = new HashMap<String, String>();
		mapAirportNames = new HashMap<String, String>();
		mapAirportIATA = new HashMap<String, Integer>();

		// read airport file and validate data
		FileInputStream fstream2 = new FileInputStream(airportFile);
		String strLine1;
		// Use DataInputStream to read binary NOT text.
		BufferedReader br1 = new BufferedReader(new InputStreamReader(fstream2));
		while ((strLine1 = br1.readLine()) != null) {
			String temp[] = strLine1.split(",");
			// check for empty records
			if (strLine1.isEmpty()) {
				out.write("Airport File :: Record NO : " + recNo + " :: Empty Records ");
				out.newLine();
				// check for Airport Name Format: X [3..20]
			} else if (temp[0].matches("[A-Z/ //]{3,20}") == false) {
				out.write("Airport File :: Record NO : " + recNo + " :: Airport Name : " + temp[0]);
				out.newLine();
				// check for Airport IATA/FAA code Format: XXX
			} else if (temp[1].matches("^[A-Z]{3}") == false) {
				out.write("Airport File :: Record NO : " + recNo + " :: Airport IATA/FAA code : " + temp[1]);
				out.newLine();
				// Latitude Format n.n [3..13]
			} else if (temp[2].replaceAll("[./-]", "").matches("\\d{3,13}") == false) {
				out.write("Airport File :: Record NO : " + recNo + " :: Latitude : " + temp[2]);
				out.newLine();
				// Longitude Format n.n [3..13]
			} else if (temp[3].replaceAll("[./-]", "").matches("\\d{3,13}") == false) {
				out.write("Airport File :: Record NO : " + recNo + " :: Longitude : " + temp[3]);
				out.newLine();
			} else {
				mapAirport.put(temp[1], temp[2] + "," + temp[3]);
				mapAirportNames.put(temp[1], temp[0]);
				mapAirportIATA.put(temp[1], 0);
			}
			// increment record no
			recNo++;

		}
		br1.close();

		// read passenger file and validate data
		passengerDataFile = new ArrayList<>();

		BufferedReader br = new BufferedReader(new FileReader(inputfile));
		String strLine = null;
		recNo = 0;
		while ((strLine = br.readLine()) != null) {
			String temp[] = strLine.split(",");
			// check for missing records
			if (temp[0].isEmpty() && temp[1].isEmpty() && temp[2].isEmpty() && temp[3].isEmpty()) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Empty Records");
				out.newLine();
				// check for Passenger id Format: XXXnnnnXXn
			} else if (temp[0].matches("^[A-Z]{3}\\d{4}[A-Z]{2}\\d{1}") == false) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Passenger ID : " + temp[0]);
				out.newLine();
				// check for Flight id Format: XXXnnnnX
			} else if (temp[1].matches("^[A-Z]{3}\\d{4}[A-Z]{1}") == false) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Flight ID : " + temp[1]);
				out.newLine();
				// check for Airport IATA/FAA code Format: XXX
			} else if (temp[2].matches("^[A-Z]{3}") == false) {
				out.write("Passenger File :: Record NO : " + recNo + " :: From airport IATA/FAA code : " + temp[2]);
				out.newLine();
				// check for Airport IATA/FAA code Format: XXX
			} else if (temp[3].matches("^[A-Z]{3}") == false) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Destination airport IATA/FAA code : "
						+ temp[3]);
				out.newLine();
				// check for Departure time (GMT) Format: n[10]
			} else if (temp[4].matches("\\d{10}") == false) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Departure time (GMT) : " + temp[4]);
				out.newLine();
				// check for Total flight time Format: n[1..4]
			} else if (temp[5].matches("\\d{1,4}") == false) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Total flight time (mins) : " + temp[5]);
				out.newLine();
				// cross check passenger from airport IATA/FAA code with Airport data file
			} else if (mapAirportNames.get(temp[2]) == null) {
				out.write("Passenger File :: Record NO : " + recNo + " :: Passenger From airport IATA/FAA code : "
						+ temp[2]);
				out.newLine();
				// cross check passenger desination airport IATA/FAA code with Airport data file
			} else if (mapAirportNames.get(temp[3]) == null) {
				out.write("Passenger File :: Record NO : " + recNo
						+ " :: Passenger Destination airport airport IATA/FAA code : " + temp[3]);
				out.newLine();
			} else {
				passengerDataFile.add(strLine);
			}

			recNo++;
		}
		br.close();
		out.close();
	}

	/*
	 * :: Main program to be call for sequence of execution of user programs :
	 */
	public static void main(int args[], String argstr[], TreeMap<String, Integer> argsmap1,
			TreeMap<String, Double> argsmap2[]) {

		// variable initialisation
		int count = 0;
		int threadNum = args[1]; // No Of Parallel Processing
		passengerFile = argstr[0]; // "C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";
		airportFile = argstr[1]; // "C:\\Users\\nirupam.lokhande\\Downloads\\AComp_Passenger_data_no_error.csv";

		MapReduce.jobId = args[0]; // Job No
		MapReduce.flightId = argstr[2]; // Flight ID

		// Parallel Processing
		ExecutorService executor = Executors.newFixedThreadPool(threadNum);
		List<FutureTask<Integer>> taskList = new ArrayList<FutureTask<Integer>>();

		try {

			// Step 1 : Cleansing / Preprocessing
			cleansing(passengerFile);
			// Step 2: Split files into multiple files or parallel processing
			FileSplit(threadNum);
			// Step 3: Map/COmbiner and Parallel Execution
			try {
				for (int i = 0; i < threadNum; i++) {
					int fileNo = i; // files

					// Parallel Execution
					FutureTask<Integer> Task = new FutureTask<Integer>(new Callable<Integer>() {
						@Override
						public Integer call() {
							try {
								// Mapper
								MapReduce.readFileandMap(fileNo);
								// Combiner
								MapReduce.combine(fileNo);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							return 1;
						}
					});

					taskList.add(Task);
					executor.execute(Task);
				}

				// Wait until all results are available and combine them at the same time
				for (int j = 0; j < threadNum; j++) {
					FutureTask<Integer> futureTask = taskList.get(j);
					count += futureTask.get();

					if (count == threadNum) {
						// Step 4: sort Shuffle
						MapReduce.sortShuffle(threadNum);
						// Step 5: Reducer
						MapReduce.reduce(argsmap1, argsmap2);
					}
				}
				executor.shutdown();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
