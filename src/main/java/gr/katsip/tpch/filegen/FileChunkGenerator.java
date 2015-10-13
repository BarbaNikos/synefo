package gr.katsip.tpch.filegen;

import java.io.*;

/**
 * Created by katsip on 10/13/2015.
 */
public class FileChunkGenerator {

    public static void main(String[] args) throws IOException {
        String fileName = null;
        String outputDirectory = null;
        Integer numberOfFiles = 0;
        Long sizeInMegabytes = 0L;
        if (args.length < 4) {
            System.out.println("Arguments: input-file output-dir number-of-files size-of-each-file-in-MB");
            System.exit(1);
        }else {
            fileName = args[0];
            outputDirectory = args[1];
            numberOfFiles = Integer.parseInt(args[2]);
            sizeInMegabytes = Long.parseLong(args[3]);
        }
        BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
        int fileCounter = 0;
        File outputFile = new File(outputDirectory + File.separator + fileCounter + ".tbl");
        if (!outputFile.exists())
            outputFile.createNewFile();
        PrintWriter writer = new PrintWriter(new FileWriter(outputFile));
        String line;
        long byteCounter = 0L;
        while (fileCounter <= numberOfFiles) {
            if ((line = reader.readLine()) == null) {
                reader.close();
                reader = new BufferedReader(new FileReader(new File(fileName)));
                line = reader.readLine();
            }
            writer.println(line);
            byteCounter += line.length();
            if (byteCounter >= (sizeInMegabytes * 1000 * 1000)) {
                writer.flush();
                writer.close();
                fileCounter += 1;
                outputFile = new File(outputDirectory + File.separator + fileCounter + ".tbl");
                if (!outputFile.exists())
                    outputFile.createNewFile();
                writer = new PrintWriter(new FileWriter(outputFile));
                byteCounter = 0L;
            }
        }
    }
}
