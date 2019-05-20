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
    }
	
    public void flightDetailsGui(){
	JFrame flightDetails = new JFrame("Airline Flight Details");
	flightDetails.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	flightDetails.setBounds(400, 200, 450, 300);
	flightDetails.getContentPane().setLayout(null);
    }
  
}
