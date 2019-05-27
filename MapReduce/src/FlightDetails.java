package MapReduce;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;

import java.awt.*;
import java.awt.event.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
//import net.miginfocom.swing.MigLayout;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;

public class FlightDetails {
	JComboBox CBTasks, CBThreads;  
	// Flights ID List
    JList listFlightId = new JList() ;
    
    public static String filepathPassenger = "";
    public static String filepathAirport = "";
    public static String args[] = new String[3];
    public static String strargs[] = new String[3];
    public static int intargs[] = new int[2];
    //public static HashMap<String, Integer> argsmap1[] = null;
    public static TreeMap<String, Double> argsmap2[] = new TreeMap[3];
    public static TreeMap<String, Integer> argsmap1 = null;
    public static TreeMap<String, String> listmap = new TreeMap<String, String>();
    //public static HashMap<String, Double> argsmap2 = null;
    
    public static void main(String[] args){
	FlightDetails gui = new FlightDetails();
        gui.flightDetailsGui();
    }
  
    public static void passengerDetailsGui(String[] args) throws IOException{        
		
	JFrame passengerDetails = new JFrame();
	passengerDetails.setBounds(100, 100, 450, 300);
	passengerDetails.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
        JPanel passengerDetailsPanel = new JPanel();
        passengerDetailsPanel.setBounds(0, 50, 434, 132);
        passengerDetails.getContentPane().add(passengerDetailsPanel);
        passengerDetailsPanel.setLayout(null);
		
	JLabel lblPassengerDetails = new JLabel("PASSENGER DETAILS");
	lblPassengerDetails.setBounds(153, 11, 148, 14);
	passengerDetailsPanel.add(lblPassengerDetails);
	    
	JButton btnLogout = new JButton("Logout");
	btnLogout.setBounds(167, 105, 91, 23);
	passengerDetailsPanel.add(btnLogout);
		
	btnLogout.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
            	passengerDetails.dispose();
            	Login.main(args);
            }
          });    
		
        // Table
        final  JTable table = new JTable();
        DefaultTableModel model = new DefaultTableModel();
        
	JScrollPane scrollPane = new JScrollPane(table);
	scrollPane.setBounds(64, 36, 293, 58);
	passengerDetailsPanel.add(scrollPane);
        

        intargs[0] = 3; // job ID
        intargs[1] = 2; // Parallel Process
        argsmap2[0] = new TreeMap<String,Double>();
        argsmap2[1] = new TreeMap<String,Double>();
        argsmap2[2] = new TreeMap<String,Double>();
	MapReduce.main(intargs,args,argsmap1,argsmap2); 
		
	model.addColumn("Passenger Id");
	model.addColumn("Total Miles");
		
	for (String i : argsmap2[1].keySet()) {
		System.out.println(i + " " + args[2]);
		if (args[2].equals(i)) {
			System.out.println(i + " " + argsmap2[1].get(i));
				
			Object[] data = {i,argsmap2[1].get(i)};
			model.addRow(data);
		}
  	 }		
		
	table.setModel(model);
	table.setPreferredScrollableViewportSize(new Dimension(500, 70));
	table.setFillsViewportHeight(true);
		
	scrollPane.setVisible(true);
	passengerDetailsPanel.add(scrollPane);
		
	passengerDetails.getContentPane().add(BorderLayout.CENTER,passengerDetailsPanel); 
	passengerDetails.setSize(400,300);
	passengerDetails.setVisible(true);

	}
	
    public void flightDetailsGui(){
		
	JFrame flightDetails = new JFrame("Airline Flight Details");
	flightDetails.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	flightDetails.setBounds(400, 200, 450, 300);
	flightDetails.getContentPane().setLayout(null);
		
	JPanel flightDetailPanel = new JPanel();
	flightDetailPanel.setBounds(20, 36, 664, 604);
	flightDetails.getContentPane().add(flightDetailPanel);
	flightDetailPanel.setLayout(null);
		
	JLabel lblPassengerDetailsFile = new JLabel("Passenger Details File :");
	lblPassengerDetailsFile.setBounds(10, 40, 146, 14);
	flightDetailPanel.add(lblPassengerDetailsFile);
        
        JTextField strPassengerDetails = new JTextField();
        strPassengerDetails.setBounds(156, 37, 373, 20);
        flightDetailPanel.add(strPassengerDetails);
        strPassengerDetails.setColumns(10);
		
	JButton btnPassengerDetails = new JButton("Select");
	btnPassengerDetails.setBounds(534, 36, 91, 23);
	flightDetailPanel.add(btnPassengerDetails);		

        JLabel lblAirportDetails = new JLabel("Airport Details File :");
        lblAirportDetails.setBounds(10, 65, 131, 14);
	flightDetailPanel.add(lblAirportDetails);
		
	JTextField strAirportDetails = new JTextField();
	strAirportDetails.setBounds(156, 68, 373, 20);
	flightDetailPanel.add(strAirportDetails);
	strAirportDetails.setColumns(10);
		
	JButton btnAirportDetails = new JButton("Select");
	btnAirportDetails.setBounds(534, 61, 91, 23);
	flightDetailPanel.add(btnAirportDetails);
		
	JLabel lblTasks = new JLabel("Tasks :");
	lblTasks.setBounds(23, 118, 46, 14);
	flightDetailPanel.add(lblTasks);
		
	String[] sTasks ={ "No Of Flight", "Lists Of Flights ", "No Of Passenger", "Nautical Miles"};
	CBTasks = new JComboBox(sTasks);
	CBTasks.setBounds(85, 114, 138, 22);
	flightDetailPanel.add(CBTasks);		
		
	JLabel lblFlightId = new JLabel("Flight ID");
	lblFlightId.setBounds(23, 166, 58, 14);
	flightDetailPanel.add(lblFlightId);
	lblFlightId.setVisible(false);
		
	listFlightId = new JList();
	listFlightId.setBounds(85, 162, 138, 60);
	flightDetailPanel.add(listFlightId);
	listFlightId.setVisible(false);
	
        //Create the scroll pane and add the table to it.
        JScrollPane scrollPane_1 = new JScrollPane(listFlightId);
        	
	JLabel lblNoOfThreads = new JLabel("No Of Threads");
	lblNoOfThreads.setBounds(432, 118, 100, 14);
	flightDetailPanel.add(lblNoOfThreads);
		
	String[] noOfThreads = {"1", "2", "4", "6"};
	CBThreads = new JComboBox(noOfThreads);
	CBThreads.setBounds(536, 114, 73, 22);
	flightDetailPanel.add(CBThreads);
		
	JButton btnSubmit = new JButton("Submit");
	btnSubmit.setBounds(185, 260, 91, 23);
	flightDetailPanel.add(btnSubmit);
		
	JButton btnLogout = new JButton("Logout");
	btnLogout.setBounds(408, 260, 91, 23);
	flightDetailPanel.add(btnLogout);
	
	JLabel lblFlightDetails = new JLabel("FLIGHT DETAILS");
	lblFlightDetails.setBounds(288, 11, 111, 14);
	flightDetailPanel.add(lblFlightDetails);
		
	JLabel lblExecutionTime = new JLabel("Execution Time :");
	lblExecutionTime.setBounds(185, 294, 111, 14);
	flightDetailPanel.add(lblExecutionTime);
	lblExecutionTime.setVisible(false);
		
	JTextField strExecutionTime = new JTextField();
	strExecutionTime.setBounds(306, 291, 86, 20);
	flightDetailPanel.add(strExecutionTime);
	strExecutionTime.setColumns(10);
	strExecutionTime.disable();
	strExecutionTime.setVisible(false);
		
	btnPassengerDetails.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
            	//FileChooser File = new FileChooser();
            	filepathPassenger = FileChooser.main();
        		System.out.println(filepathPassenger); 
        		strPassengerDetails.setText(filepathPassenger);
        		args[0] = filepathPassenger;
        		strargs[0] = filepathPassenger;
            }
          });
        
       btnAirportDetails.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
            	//FileChooser File = new FileChooser();
            	filepathAirport  = FileChooser.main();
        		System.out.println(filepathAirport);  
        		strAirportDetails.setText(filepathAirport);
        		args[1] = filepathAirport;
        		strargs[1] = filepathAirport;
            }
          });
        
        // Back
       btnLogout.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
            	flightDetails.dispose();
            	Login.main(args);
            }
          });
	
        DefaultListModel dmFlightId = new DefaultListModel();
       
	CBTasks.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
 	
            	if(CBTasks.getSelectedIndex() == 1 ) {
            		if (strargs[0] == null) {
                		JOptionPane.showMessageDialog(null, "Please Select Passenger Details File");
                	}
                	else {
                		//String airportfile = argstr[1];//"C:\\Users\\nirupam.lokhande\\Downloads\\Top30_airports_LatLong.csv";
                        FileInputStream fstream2;
						try {
							JOptionPane.showMessageDialog(null, strargs[0]);
							fstream2 = new FileInputStream(strargs[0]);
							String strLine1;
	                        // Use DataInputStream to read binary NOT text.
	                        BufferedReader br1 = new BufferedReader(new InputStreamReader(fstream2));
	                        while ((strLine1 = br1.readLine()) != null) {
	                            String temp[] = strLine1.split(",");
	                            if (temp[1].matches("^[A-Z]{3}\\d{4}[A-Z]{1}") == true ) {
	               			      listmap.put(temp[1], temp[1]);
	                            }
	                        }
	                        
	                        br1.close();
	                        
	                        for (String i : listmap.keySet()) {
	                        	dmFlightId.addElement(i);
	                        	//System.out.println(i);
	                      	}
	                        
	                        listFlightId.setModel(dmFlightId);
	                        listFlightId.setVisible(true);
	                        lblFlightId.setVisible(true);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						

	            		scrollPane_1.setVisible(true);
	            		scrollPane_1.setBounds(88, 154, 135, 88);	
	                        flightDetailPanel.add(scrollPane_1);
				        
				flightDetails.getContentPane().add(BorderLayout.CENTER,flightDetailPanel); 
	            		flightDetails.setSize(800,800);
	            		flightDetails.setVisible(true);
                	}
            	}
            	else {
            		dmFlightId.clear();
                     	listFlightId.setVisible(false);
                     	lblFlightId.setVisible(false);
            		scrollPane_1.setVisible(false);
            		flightDetails.revalidate();
                     	flightDetails.getContentPane().add(BorderLayout.CENTER,flightDetailPanel); 
            		flightDetails.setSize(800,800);
            		flightDetails.setVisible(true);
            	}
            
            }
        });
	    
	// Table		
	final  JTable table = new JTable();
	final  JTable table_2 = new JTable();
	final  JTable table_3 = new JTable();        

	//Create the scroll pane and add the table to it.
	//Create the scroll pane and add the table to it.
	JScrollPane scrollPane = new JScrollPane(table);
	scrollPane.setBounds(85, 321, 540, 100);
	scrollPane.setVisible(false);
		            		
	JScrollPane scrollPane_2 = new JScrollPane(table_2);
	scrollPane_2.setBounds(85, 468, 255, 100);
	scrollPane_2.setVisible(false);
		
	JScrollPane scrollPane_3 = new JScrollPane(table_3);
	scrollPane_3.setBounds(355, 468, 270, 100);
	scrollPane_3.setVisible(false);
		
	JLabel lblTotalTravelledByPassenger = new JLabel("Total Travelled By Each Passenger");
	lblTotalTravelledByPassenger.setBounds(100, 448, 229, 14);
	flightDetailPanel.add(lblTotalTravelledByPassenger);
	lblTotalTravelledByPassenger.setVisible(false);
		
	JLabel lblHighestMiles = new JLabel("Passenger Having Earned The Highest Air Miles");
	lblHighestMiles.setBounds(366, 448, 243, 14);
	flightDetailPanel.add(lblHighestMiles);
	lblHighestMiles.setVisible(false);
	    
       btnSubmit.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
            	
            	argsmap1 = new TreeMap<String,Integer>();
                argsmap2[0] = new TreeMap<String,Double>();
                argsmap2[1] = new TreeMap<String,Double>();
                argsmap2[2] = new TreeMap<String,Double>();
		    
		lblTotalTravelledByPassenger.setVisible(false);
                lblHighestMiles.setVisible(false);    
            	
            	intargs[0] = CBTasks.getSelectedIndex(); // Job Id
            	
            	if (intargs[0] == 1) {
            		strargs[2] = (String) listFlightId.getSelectedValue();
            		JOptionPane.showMessageDialog(null,strargs[2]);
            	};
            	
            	if(CBThreads.getSelectedIndex() == 0 ) {
            		intargs[1] = 1; // No Of Parallel Processing
            	}
            	else if(CBThreads.getSelectedIndex() == 1 ) {
            		intargs[1] = 2; // No Of Parallel Processing
            	}
            	else if(CBThreads.getSelectedIndex() == 2 ) {
            		intargs[1] = 4; // No Of Parallel Processing
            	}
            	else if(CBThreads.getSelectedIndex() == 3 ) {
            		intargs[1] = 6; // No Of Parallel Processing
            	}
            	
            	if (strargs[0] == null || strargs[1] == null) {
            		JOptionPane.showMessageDialog(null, "Please select both files");
            	}
            	else {
            		
            		FileWriter fstream1 = null;
					
            		try {
				fstream1 = new FileWriter(filepathPassenger.replace(".csv", "") + "_OUTPUTFILE_" + intargs[0] + ".csv");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	                BufferedWriter out = new BufferedWriter(fstream1); 					        
	                
            		long startTime = System.currentTimeMillis();
            		MapReduce.main(intargs,strargs,argsmap1,argsmap2); 
            		long endTime = System.currentTimeMillis();
            		strExecutionTime.setText(Long.toString(endTime - startTime));
            		System.out.println("strExecutionTime : " + strExecutionTime.getText());
            		//System.out.println(argsmap2);

            		                    
                	if (intargs[0] == 0 ) {
                		System.out.println(intargs[0]);
                		DefaultTableModel model = new DefaultTableModel();
                		model.addColumn("Airports");
                		model.addColumn("NO Of flights");
                		
                		// header
                		try {
					out.write("Airports" + "," + "NO Of flights");
					out.newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   //acquring the first column
                        
                        
                		for (String i : argsmap1.keySet()) {
        	        		try {
						out.write(i + "," + argsmap1.get(i));
						out.newLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}   //acquring the first column
                            
                          
        	        	 	Object[] data = {i,argsmap1.get(i)};
        	        	 	model.addRow(data);
        	      	  	}
                		 
	                	table.setModel(model);
				
				if (scrollPane_1 != null) {
                    			scrollPane_1.setVisible(false);
                    		}
	                	if (scrollPane_2 != null) {
	                		scrollPane_2.setVisible(false);
	                	}
	                	
	                	if (scrollPane_3 != null) {
                			scrollPane_3.setVisible(false);
                		}	
            		}
            		else if(intargs[0] == 1 ) {            			
            			
            			// header
                		try {
					out.write("PassengerId,DeparturePort,ArrivalPort,DepartureTime,ArrivalTime,FlightTime");
			            	out.newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   //acquring the first column
                		
                		
            			System.out.println(intargs[0]);
            			System.out.println(argsmap1);
				
            			DefaultTableModel model = new DefaultTableModel();
            			model.addColumn("Passenger Id");
                		model.addColumn("Departure Port");
                		model.addColumn("Arrival Port");
                		model.addColumn("Departure Time");
                		model.addColumn("Arrival Time");
                		model.addColumn("Flight Time");
                		
                		for (String i : argsmap1.keySet()) {
        	        	    String temp[] = i.split("-");
        	        	    Object[] data = {temp[0],temp[1],temp[2],temp[3],temp[4],temp[5]};
        	        	    model.addRow(data);
        	        	    
        	        	    try {
					out.write(temp[0]+","+temp[1]+","+temp[2]+","+temp[3]+","+temp[4]+","+temp[5]);
					out.newLine();
				     } catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				     }   //acquring the first column
        	        	    
        	      	     	}
                		
                		table.setModel(model);
                		
                		table.setModel(model);
	                	if (scrollPane_2 != null) {
	                		scrollPane_2.setVisible(false);
	                	}
	                	
	                	if (scrollPane_3 != null) {
                			scrollPane_3.setVisible(false);
                		}
            		}
            		else if(intargs[0] == 2 ) {
            			
            			// header
                		try {
					out.write("Flight Id,No Of Passenger");
			            	out.newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   //acquring the first column
                		
                		DefaultTableModel model = new DefaultTableModel();
            			model.addColumn("Flight Id");
            			model.addColumn("No Of Passenger");
            			
            			for (String i : argsmap1.keySet()) {
        	        	    //String temp[] = i.split(",");
        	        	    Object[] data = {i,argsmap1.get(i)};
        	        	    model.addRow(data);
        	        	    
        	        	    try {
					out.write(i+","+argsmap1.get(i));
					out.newLine();
				    } catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				    }   //acquring the first column
        	        	    
        	      	     	}
            			
                		table.setModel(model);	
                		
                		if (scrollPane_1 != null) {
                    			scrollPane_1.setVisible(false);
                    		}
				
	                	if (scrollPane_2 != null) {
	                		scrollPane_2.setVisible(false);
	                	}
	                	
	                	if (scrollPane_3 != null) {
                			scrollPane_3.setVisible(false);
                		}
            			
                   	}
            		else if(intargs[0] == 3 ) {
            			
				if (scrollPane_1 != null) {
                    			scrollPane_1.setVisible(false);
                    		}
				
				lblTotalTravelledByPassenger.setVisible(true);
                		lblHighestMiles.setVisible(true);
				
            			// header
                		try {
					out.write("Flight Id,Total Miles");
			            	out.newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   //acquring the first column
                		
                		DefaultTableModel model = new DefaultTableModel();
                        	DefaultTableModel model_2 = new DefaultTableModel();
                       		DefaultTableModel model_3 = new DefaultTableModel();
                		
            			model.addColumn("Flight Id");
                		model.addColumn("Total Miles");
                		
                		for (String i : argsmap2[0].keySet()) {
        	        	    Object[] data = {i,argsmap2[0].get(i)};
        	        	    model.addRow(data);
        	        	    
        	        	    try {
					out.write(i+","+argsmap2[0].get(i));
					out.newLine();
				    } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				    }   //acquring the first column
        	        	    
        	      	     	}
                		
                		table.setModel(model);
                		                		
                		// header
                		try {
					out.write("Passenger Id,Total Miles");
			            	out.newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   //acquring the first column
                		
                		model_2.addColumn("Passenger Id");
                		model_2.addColumn("Total Miles");
                		
                		for (String i : argsmap2[1].keySet()) {
        	        	    Object[] data = {i,argsmap2[1].get(i)};
        	        	    model_2.addRow(data);
        	        	    
        	        	    try {
					out.newLine();    
					out.write(i+","+argsmap2[1].get(i));
					out.newLine();
				    } catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				    }   //acquring the first column
        	      	        }
                		
                		table_2.setModel(model_2);
                		table_2.setPreferredScrollableViewportSize(new Dimension(500, 70));
                		table_2.setFillsViewportHeight(true);            		
                		scrollPane_2.setVisible(true);
                		flightDetailPanel.add(scrollPane_2);
                		
                		
                		// header
                		try {
					out.newLine();
					out.write("Passenger Id,Highest Total Miles");
			            	out.newLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}   //acquring the first column
                		
                		model_3.addColumn("Passenger Id");
                		model_3.addColumn("Highest Total Miles");
                		
                		for (String i : argsmap2[2].keySet()) {
        	        	    Object[] data = {i,argsmap2[2].get(i)};
        	        	    model_3.addRow(data);
        	        	    
        	        	    try {
					out.write(i+","+argsmap2[2].get(i));
					out.newLine();
				    } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				    }   //acquring the first column
        	      	        }              		
                		
                		table_3.setModel(model_3);
                		table_3.setPreferredScrollableViewportSize(new Dimension(500, 70));
                		table_3.setFillsViewportHeight(true);            		
                		scrollPane_3.setVisible(true);
                		flightDetailPanel.add(scrollPane_3);
                   	}

            		try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}      			
                	            	
            		table.setPreferredScrollableViewportSize(new Dimension(500, 70));
            		table.setFillsViewportHeight(true); 
            		scrollPane.setVisible(true);
            		flightDetailPanel.add(scrollPane);
            		
            		flightDetails.getContentPane().add(BorderLayout.CENTER,flightDetailPanel); 
            		flightDetails.setSize(800,800);
            		flightDetails.setVisible(true);
            	} 	
            }
          });
        
	flightDetails.setSize(700,700);
	flightDetails.setVisible(true);
    }
  
}
