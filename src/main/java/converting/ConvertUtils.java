package converting;

import org.apache.hadoop.fs.Path;
import parquet.Log;
import parquet.example.data.Group;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.*;
import java.util.Arrays;
import java.util.regex.Pattern;

public class ConvertUtils {
  private static final Log LOG = Log.getLog(ConvertUtils.class);
    public static final String CSV_DELIMITER= "; ";
    private static String readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder stringBuilder = new StringBuilder();
        try {
            String line = null;
            String ls = System.getProperty("line.separator");
            while ((line = reader.readLine()) != null ) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } finally {
            Utils.closeQuietly(reader);
        }
        return stringBuilder.toString();
    }
    public static String getSchema(File csvFile) throws IOException {
        String fileName = csvFile.getName().substring(
                0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        return readFile(schemaFile.getAbsolutePath());
    }
    public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
        convertCsvToParquet(csvFile, outputParquetFile, false);
    }
    public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
        LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
        String rawSchema = getSchema(csvFile);
        if(outputParquetFile.exists()) {
            throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + " already exists");
        }
        Path path = new Path(outputParquetFile.toURI());
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);
        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        int lineNumber = 0;
        try {
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                writer.write(Arrays.asList(fields));
                ++lineNumber;
            }
            writer.close();
        } finally {
            LOG.info("Number of lines: " + lineNumber);
            Utils.closeQuietly(br);
        }
    }
    private static void writeGroup(BufferedWriter w, Group g, MessageType schema)
            throws IOException{
        for (int j = 0; j < schema.getFieldCount(); j++) {
            if (j > 0) {
                w.write(CSV_DELIMITER);
            }
            String valueToString = g.getValueToString(j, 0);
            w.write(valueToString);
        }
        w.write('\n');
    }
    public static void main(String[] args) {
//        File ParquetFile ;
//        File CSVFile ;
//        ParquetFile = new File ("C:/Users/ekaterina_semenova/Downloads/all/destinations/destinations.parquet" );
//        CSVFile = new File ("C:/Users/ekaterina_semenova/Downloads/all/destinations/destinations.csv");
//
       try {
            convertCsvToParquet(new File(args[0]),new File(args[1]));
//            convertCsvToParquet(CSVFile,ParquetFile);
        } catch (IOException e) {
// TODO Auto-generated catch block
            e.printStackTrace();
        }
    }




}