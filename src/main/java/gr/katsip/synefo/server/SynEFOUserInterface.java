package gr.katsip.synefo.server;

import gr.katsip.cestorm.db.CEStormDatabaseManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class SynEFOUserInterface implements Runnable {

	private boolean exitFlag;
	
	private HashMap<String, ArrayList<String>> physicalTopology;
	
	private ZooMaster beastMaster;
	
	private CEStormDatabaseManager ceDb;
	
	private HashMap<String, Integer> operatorNameToIdMap;

	public SynEFOUserInterface(ZooMaster beastMaster, HashMap<String, ArrayList<String>> physicalTopology) {
		this.beastMaster = beastMaster;
		this.physicalTopology = new HashMap<String, ArrayList<String>>(physicalTopology);
		ceDb = new CEStormDatabaseManager("","","");
	}

	public void run() {
		exitFlag = false;
		BufferedReader _input = new BufferedReader(new InputStreamReader(System.in));
		String command = null;
		System.out.println("+efo Server started (UI). Type help for the list of commands");
		operatorNameToIdMap = ceDb.getOperatorNameToIdentifiersMap(queryId);
		while(exitFlag == false) {
			try {
				System.out.print("+efo>");
				command = _input.readLine();
				if(command != null && command.length() > 0) {
					String tokens[] = command.split(" ");
					String comm = tokens[0];
					parseCommand(comm, tokens);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void parseCommand(String command, String[] tokens) {
		if(command.equals("scale-out")) {
			String compOne = null;
			if(tokens.length > 1) {
				compOne = tokens[1];
				if(physicalTopology.containsKey(compOne) == false) {
					System.out.println("+efo error: Need to define an existing component-A when \"scale-out\" is issued.");
					return;
				}
			}else {
				System.out.println("+efo error: Need to define component-A (will {remove|add} task from/to downstream) when \"scale-out\" is issued.");
				return;
			}
			String compTwo = null;
			if(tokens.length > 2) {
				compTwo = tokens[2];
				if(physicalTopology.containsKey(compTwo) == false) {
					System.out.println("+EFO error: Need to define an existing component-B when \"scale-out\" is issued.");
					return;
				}
			}else {
				System.out.println("+efo error: Need to define component-B (will be {remove|add}-ed from/to component-A downstream) when \"scale-out\" is issued.");
				return;
			}
			/**
			 * Issue the command for scaling out
			 */
			if(physicalTopology.get(compOne).lastIndexOf(compTwo) == -1) {
				System.out.println("+efo error: " + compTwo + " is not an available downstream task of " + compOne + ".");
				return;
			}
			synchronized(beastMaster) {
				/**
				 * If the node is not active.
				 */
				if(beastMaster.scaleFunction.getActiveTopology().get(compOne).lastIndexOf(compTwo) < 0 && 
						beastMaster.scaleFunction.getActiveTopology().containsKey(compTwo) == false) {
					beastMaster.scaleFunction.addInactiveNode(compTwo);
					String scaleOutCommand = "ADD~" + compTwo;
					String activateCommand = "ACTIVATE~" + compTwo;
					ArrayList<String> peerParents = new ArrayList<String>(ScaleFunction.getInverseTopology(physicalTopology).get(compTwo));
					peerParents.remove(peerParents.indexOf(compOne));
					beastMaster.setScaleCommand(compOne, scaleOutCommand, peerParents, activateCommand);
				}
			}
		}else if(command.equals("scale-in")) {
			String compOne = null;
			if(tokens.length > 1) {
				compOne = tokens[1];
				if(physicalTopology.containsKey(compOne) == false) {
					System.out.println("+efo error: Need to define an existing component-A when \"scale-out\" is issued.");
					return;
				}
			}else {
				System.out.println("+efo error: Need to define component-A (will {remove|add} task from/to downstream) when \"scale-out\" is issued.");
				return;
			}
			String compTwo = null;
			if(tokens.length > 2) {
				compTwo = tokens[2];
				if(physicalTopology.containsKey(compTwo) == false) {
					System.out.println("+EFO error: Need to define an existing component-B when \"scale-out\" is issued.");
					return;
				}
			}else {
				System.out.println("+efo error: Need to define component-B (will be {remove|add}-ed from/to component-A downstream) when \"scale-out\" is issued.");
				return;
			}
			/**
			 * Issue the command for scaling out
			 */
			if(physicalTopology.get(compOne).lastIndexOf(compTwo) == -1) {
				System.out.println("+efo error: " + compTwo + " is not an available downstream task of " + compOne + ".");
				return;
			}else if(beastMaster.scaleFunction.getActiveTopology().get(compOne).lastIndexOf(compTwo) == -1) {
				System.out.println("+efo error: " + compTwo + " is not an active downstream task of " + compOne + ".");
				return;
			}
			synchronized(beastMaster) {
				/**
				 * If the node is active.
				 */
				if(beastMaster.scaleFunction.getActiveTopology().get(compOne).lastIndexOf(compTwo) >= 0 && 
						beastMaster.scaleFunction.getActiveTopology().containsKey(compTwo) == true) {
					beastMaster.scaleFunction.removeActiveNode(compTwo);
					String scaleInCommand = "REMOVE~" + compTwo;
					String deActivateCommand = "DEACTIVATE~" + compTwo;
					ArrayList<String> peerParents = new ArrayList<String>(ScaleFunction.getInverseTopology(physicalTopology).get(compTwo));
					if(peerParents.indexOf(compOne) != -1) {
						peerParents.remove(peerParents.indexOf(compOne));
						beastMaster.setScaleCommand(compOne, scaleInCommand, peerParents, deActivateCommand);
					}
				}
			}
		}else if(command.equals("active-top")) {
			HashMap<String, ArrayList<String>> activeTopologyCopy = new HashMap<String, ArrayList<String>>(
					beastMaster.scaleFunction.getActiveTopology());
			Iterator<Entry<String, ArrayList<String>>> itr = activeTopologyCopy.entrySet().iterator();
			while(itr.hasNext()) {
				Entry<String, ArrayList<String>> entry = itr.next();
				String task = entry.getKey();
				ArrayList<String> downStream = entry.getValue();
				System.out.println("\tTask: " + task + " down stream: ");
				for(String t : downStream) {
					System.out.println("\t\t" + t);
				}
			}
		}else if(command.equals("physical-top")) {
			HashMap<String, ArrayList<String>> physicalTopologyCopy = new HashMap<String, ArrayList<String>>(physicalTopology);
			Iterator<Entry<String, ArrayList<String>>> itr = physicalTopologyCopy.entrySet().iterator();
			while(itr.hasNext()) {
				Entry<String, ArrayList<String>> entry = itr.next();
				String task = entry.getKey();
				ArrayList<String> downStream = entry.getValue();
				System.out.println("\tTask: " + task + " down stream: ");
				for(String t : downStream) {
					System.out.println("\t\t" + t);
				}
			}
		}else if(command.equals("help")) {
			/**
			 * Print help instructions
			 */
			System.out.println("Available commands:");
			System.out.println("\t scale-out <component-one> <component-two>: action:{add,remove}");
			System.out.println("\t\t component-one: task that will have downstream modified");
			System.out.println("\t\t component-two: task that will either be added");
			System.out.println("\t scale-in <component-one> <component-two>: action:{add,remove}");
			System.out.println("\t\t component-one: task that will have downstream modified");
			System.out.println("\t\t component-two: task that will be removed");
			System.out.println("\t active-top: Prints out the current working topology");
			System.out.println("\t physical-top: Prints out the physical topology");
			System.out.println("\t quit: self-explanatory");
		}else if(command.equals("quit")) {
			exitFlag = true;
		}else {
			System.out.println("Unrecognized command. Type help to see list of commands");
		}
	}

}
