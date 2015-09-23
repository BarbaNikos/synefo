package gr.katsip.synefo.storm.api;

import java.io.Serializable;

/**
 * Created by katsip on 9/22/2015.
 */
public class GenericTriplet<T1, T2, T3> implements Serializable {

    public T1 first;

    public T2 second;

    public T3 third;

    public GenericTriplet() {
        this.first = null;
        this.second = null;
    }

    public GenericTriplet(T1 first, T2 second, T3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}
