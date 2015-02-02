package gr.katsip.synefo.storm.lib;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Topology implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6158556030423097053L;
	
	public HashMap<String, ArrayList<String>> _topology;

	public Topology() {
		_topology = new HashMap<String, ArrayList<String>>();
	}

	@Override
	public String toString() {
		return "Topology [_topology=" + _topology + "]";
	}
}
