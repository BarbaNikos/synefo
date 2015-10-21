package gr.katsip.deprecated;

import java.io.Serializable;
import java.util.Comparator;

public class StringComparator implements Comparator<String>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7669115486458598013L;

	@Override
	public int compare(String s1, String s2) {
		if(s1.compareToIgnoreCase(s2) == 0)
			return 0;
		else if(s1.compareToIgnoreCase(s2) < 0)
			return 1;
		else
			return 2;
	}

}
