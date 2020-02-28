import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
        FileAccess hadoop = new FileAccess("hdfs://af1bc3dc06a8:8020");

        //hadoop.createFile("/test2.txt");
       // Test test = new Test();
        hadoop.append("/test2.txt", "new content");


        hadoop.createFile("/file.txt");
        hadoop.createDir("/newdir");

        hadoop.delete("/file.txt");
        hadoop.delete("/newdir");



        //    hadoop.create("/test.txt");



//        if (hadoop.isDirectory("/test2.txt")) {
//            System.out.println("This is directory");
//        } else {
//            System.out.println("This is file");
//        }
//        hadoop.append("/test.txt", "new append0");
//
//        hadoop.read("/test.txt");


        //hadoop.append("/test.txt", "new append");

//        Configuration configuration = new Configuration();
//        configuration.set("dfs.client.use.datanode.hostname", "true");
//        System.setProperty("HADOOP_USER_NAME", "root");
//
//        FileSystem hdfs = FileSystem.get(
//            new URI("hdfs://af1bc3dc06a8:8020"), configuration
//        );
//        Path file = new Path("hdfs://af1bc3dc06a8:8020/test/file.txt");
//
//        if (hdfs.exists(file)) {
//            hdfs.delete(file, true);
//        }
//
//        Path file2 = new Path("/test3");
//
//
//
//        hdfs.mkdirs(file2);
//
//        if (hdfs.isDirectory(file2)) {
//            System.out.println("This is directory!!!");
//        }
//
//        OutputStream os = hdfs.create(file);
//        BufferedWriter br = new BufferedWriter(
//            new OutputStreamWriter(os, "UTF-8")
//        );
//
//        for(int i = 0; i < 10_000_000; i++) {
//            br.write(getRandomWord() + " ");
//        }
//
//        br.flush();
//        br.close();
//        hdfs.close();
//
//        hdfs.
    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
