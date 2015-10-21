package gr.katsip.synefo.storm.operators.crypstream;

import gr.katsip.deprecated.AbstractOperator;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class Paillier implements AbstractOperator, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2491691028039123050L;

	private BigInteger p,  q,  lambda;
	
	public BigInteger n;
	
	public BigInteger nsquare;
	
	private BigInteger g;
	
	private int bitLength;

	public Paillier() {
        KeyGeneration(512, 64);
    }
	
	/**
     * Sets up the public key and private key.
     * @param bitLengthVal number of bits of modulus.
     * @param certainty The probability that the new BigInteger represents a prime number will exceed (1 - 2^(-certainty)). The execution time of this constructor is proportional to the value of this parameter.
     */    
    public void KeyGeneration(int bitLengthVal, int certainty) {
        bitLength = bitLengthVal;
        /*Constructs two randomly generated positive BigIntegers that are probably prime, with the specified bitLength and certainty.*/
        p = new BigInteger(bitLength / 2, certainty, new Random());
        q = new BigInteger(bitLength / 2, certainty, new Random());

        n = p.multiply(q);
        nsquare = n.multiply(n);

        g = new BigInteger("2");
        lambda = p.subtract(BigInteger.ONE).multiply(q.subtract(BigInteger.ONE)).divide(
                p.subtract(BigInteger.ONE).gcd(q.subtract(BigInteger.ONE)));
        /* check whether g is good.*/
        if (g.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).gcd(n).intValue() != 1) {
            System.out.println("g is not good. Choose g again.");
            System.exit(1);
        }
    }
    
    /**
     * Encrypts plaintext m. ciphertext c = g^m * r^n mod n^2. This function explicitly requires random input r to help with encryption.
     * @param m plaintext as a BigInteger
     * @param r random plaintext to help with encryption
     * @return ciphertext as a BigInteger
     */
    public BigInteger Encryption(BigInteger m, BigInteger r) {
        return g.modPow(m, nsquare).multiply(r.modPow(n, nsquare)).mod(nsquare);
    }

    /**
     * Encrypts plaintext m. ciphertext c = g^m * r^n mod n^2. This function automatically generates random input r (to help with encryption).
     * @param m plaintext as a BigInteger
     * @return ciphertext as a BigInteger
     */
    public BigInteger Encryption(BigInteger m) {
        BigInteger r = new BigInteger(bitLength, new Random());
        return g.modPow(m, nsquare).multiply(r.modPow(n, nsquare)).mod(nsquare);

    }

    /**
     * Decrypts ciphertext c. plaintext m = L(c^lambda mod n^2) * u mod n, where u = (L(g^lambda mod n^2))^(-1) mod n.
     * @param c ciphertext as a BigInteger
     * @return plaintext as a BigInteger
     */
    public BigInteger Decryption(BigInteger c) {
        BigInteger u = g.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).modInverse(n);
        return c.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).multiply(u).mod(n);
    }
    
    /**
     * Returns the public key to send to the data consumer. 
     * @param
     * @returns
     */
    public BigInteger[] returnPublicInfo(){
    	BigInteger[] retList = new BigInteger[3];
    	retList[0] = n;
    	retList[1] = nsquare;
    	retList[2] = g;
    	
    	return retList;
    }

	@Override
	public void init(List<Values> stateValues) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Values> getStateValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getStateSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		// TODO Auto-generated method stub
		
	}
}
