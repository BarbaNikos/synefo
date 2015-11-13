package gr.katsip.file.filegen;

import java.io.*;

/**
 * Created by katsip on 11/9/2015.
 */
public class BalancedFileGenerator {

    private static int scale;

    private static String[] inputFile;

    private static double[] outputRate;

    private static int[] checkpoint;

    private static float windowInMinutes;

    private static long slideInMilliSeconds;

    public static void configure(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
            scale = Integer.parseInt(reader.readLine().split("=")[1]);
            inputFile = reader.readLine().split("=")[1].split(",");
            String[] strOutputRate = reader.readLine().split("=")[1].split(",");
            String[] strCheckpoint = reader.readLine().split("=")[1].split(",");
            outputRate = new double[strOutputRate.length];
            checkpoint = new int[strCheckpoint.length];
            for (int i = 0; i < strOutputRate.length; i++) {
                outputRate[i] = Double.parseDouble(strOutputRate[i]);
                checkpoint[i] = Integer.parseInt(strCheckpoint[i]);
            }
            windowInMinutes = Float.parseFloat(reader.readLine().split("=")[1]);
            slideInMilliSeconds = Long.parseLong(reader.readLine().split("=")[1]);
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("arguments: driver-conf output-directory");
            System.exit(1);
        }
        int numberOfTuples = 0;
        configure(args[0]);
        for (int i = 0; i < checkpoint.length - 1; i++) {
            System.out.println("for interval between [" + checkpoint[i] + "," + checkpoint[i+1] + "] and rate: " +
            outputRate[i] + " will produce " + (outputRate[i] * (checkpoint[i+1] - checkpoint[i])) + " tuples per file.");
            numberOfTuples += (outputRate[i] * (checkpoint[i+1] - checkpoint[i]));
        }
        System.out.println("for the interval between [" + checkpoint[checkpoint.length - 1] + ",+INF] tuples per file: " +
                (outputRate[outputRate.length - 1] * 4 * windowInMinutes * 60));
        numberOfTuples += (outputRate[outputRate.length - 1] * 4 * windowInMinutes * 60);
        System.out.println("number of tuples produced per file: " + numberOfTuples);
        File inner = new File(args[1] + File.separator + "inner.tbl");
        File outer = new File(args[1] + File.separator + "outer.tbl");
        if (inner.exists())
            inner.delete();
        if (outer.exists())
            outer.delete();
        inner.createNewFile();
        outer.createNewFile();
        PrintWriter innerWriter = new PrintWriter(new FileWriter(inner));
        PrintWriter outerWriter = new PrintWriter(new FileWriter(outer));
        long counter = 0;
        while (counter < numberOfTuples) {
            innerWriter.println("1");
            outerWriter.println("1");
            innerWriter.println("2");
            outerWriter.println("2");
            counter += 2;
        }
        innerWriter.flush();
        outerWriter.flush();
        innerWriter.close();
        outerWriter.close();
    }
}
