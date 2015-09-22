package gr.katsip.synefo.storm.api;

import java.io.Serializable;

/**
 * Created by katsip on 9/22/2015.
 */
public class GenericPair<T1, T2> implements Serializable {

    public T1 first;

    public T2 second;

    public GenericPair() {
        this.first = null;
        this.second = null;
    }

    public GenericPair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }
}
