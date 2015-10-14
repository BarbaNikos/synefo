package gr.katsip.synefo.storm.api;

import java.io.Serializable;

public class Pair<T1, T2> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5802815705598462469L;

	public T1 lowerBound;

	public T2 upperBound;

	public Pair() {

	}

	public Pair(T1 t1, T2 t2) {
		lowerBound = t1;
		upperBound = t2;
	}
}
