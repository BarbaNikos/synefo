package gr.katsip.synefo.utils;

import java.io.Serializable;
import java.util.HashMap;

public class SynefoMessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 75042577534490656L;

	public enum Type {
		DUMMY,
		REG,
		STAT,
		SCLOUT
	}
	
	public Type _type;

	public HashMap<String, String> _values;
	
	public SynefoMessage() {
		_type = Type.DUMMY;
		_values = new HashMap<String, String>();
	}
}
