import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception {
        FileAccess hadoop = new FileAccess("hdfs://af1bc3dc06a8:8020");
        hadoop.createDir("/dir1");
        hadoop.createDir("/dir2");
        System.out.println(hadoop.isDirectory("/dir1"));
        hadoop.createFile("/dir1/test.txt");
        hadoop.createFile("/dir2/dir2test.txt");
        hadoop.append("/dir1/test.txt", "first");
        hadoop.append("/dir2/dir2test.txt", "second");
        hadoop.list("/").forEach(System.out::println);
        System.out.println(hadoop.read("/dir1/test.txt"));
        System.out.println(hadoop.read("/dir2/dir2test.txt"));
        hadoop.delete("/dir1/test.txt");
        hadoop.delete("/dir1");
        hadoop.delete("/dir2");

        hadoop.closeFs();
    }
}
