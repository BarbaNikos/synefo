package gr.katsip.file.filegen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by katsip on 11/9/2015.
 */
public class BalancedFileGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("arguments: output-directory number-of-tuples");
            System.exit(1);
        }
        long numberOfTuples = Long.parseLong(args[1]);
        File inner = new File(args[0] + File.separator + "inner.tbl");
        File outer = new File(args[0] + File.separator + "outer.tbl");
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
            counter++;
        }
        innerWriter.flush();
        outerWriter.flush();
        innerWriter.close();
        outerWriter.close();
    }
}
