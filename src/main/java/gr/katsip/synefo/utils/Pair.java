package gr.katsip.synefo.utils;

import java.io.Serializable;

public class Pair<T1, T2> implements Serializable {

	public T1 first;

	public T2 second;

	public Pair() {

	}

	public Pair(T1 t1, T2 t2) {
		first = t1;
		second = t2;
	}
}
