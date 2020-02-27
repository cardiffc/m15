import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
        FileAccess hadoop = new FileAccess("hdfs://af1bc3dc06a8:8020");
        hadoop.create("/test4");

        if (hadoop.isDirectory("/test4")) {
            System.out.println("This is directory");
        } else {
            System.out.println("This is file");
        }



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
