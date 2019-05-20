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
    public static HashMap<String, Double> argsmap2[] = new HashMap[3];
    public static HashMap<String, Integer> argsmap1 = null;
    public static HashMap<String, String> listmap = new HashMap<String, String>();
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
		
        // Table
        final  JTable table = new JTable();
        DefaultTableModel model = new DefaultTableModel();
        
	JScrollPane scrollPane = new JScrollPane(table);
	scrollPane.setBounds(64, 36, 293, 58);
	passengerDetailsPanel.add(scrollPane);
        

        intargs[0] = 3; // job ID
        intargs[1] = 2; // Parallel Process
        argsmap2[0] = new HashMap<String,Double>();
        argsmap2[1] = new HashMap<String,Double>();
        argsmap2[2] = new HashMap<String,Double>();
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
    }
  
}
