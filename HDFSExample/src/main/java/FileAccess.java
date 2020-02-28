import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileAccess
{
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    public FileSystem hdfs;
    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        hdfs = FileSystem.get(
                new URI(rootPath), configuration
        );

    }

    public void print() {}

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void createFile(String path) throws IOException {
        Path newPath = getPath(path);
        if (!hdfs.exists(newPath)) {
            hdfs.createNewFile(newPath);
        } else {
            System.out.println("File exists");
        }


    }

    public void createDir(String path) throws IOException {
        Path dir = getPath(path);
        if (!hdfs.exists(dir)) {
            hdfs.mkdirs(dir);
        } else {
            System.out.println("Directory exists");
        }
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {
        FSDataOutputStream out = hdfs.append(getPath(path));

        out.writeUTF(content);
        out.flush();
        out.close();
//        Path file = new Path(path);
//        if (hdfs.exists(file) && !hdfs.isDirectory(file)) {
//            OutputStream os = hdfs.append(file);
//            BufferedWriter br = new BufferedWriter(
//                    new OutputStreamWriter(os, "UTF-8")
//            );
//            br.write(content);
//            br.flush();
//            br.close();
//        } else {
//            System.out.println("Append operation could not be permitted!");
//        }
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {

        FSDataInputStream input = hdfs.open(getPath(path));
        System.out.println(input.toString());
//        Path file = new Path(path);
//
//        File test = new File(hdfs.open(file));
//
//
//
//        BufferedReader fsdis = new BufferedReader(new InputStreamReader(hdfs.open(file)));
//        System.out.println(fsdis.readLine());


        //new InputStreamReader(new FileInputStream(String.valueOf(hdfs.open(file))));

        //System.out.println(fsdis.readLine());


        // FileReader fr = new FileReader();

        return null;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException {
        hdfs.delete(getPath(path));

    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        return hdfs.isDirectory(getPath(path));
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path)
    {
       // hdfs.get
        return null;
    }

    private Path getPath (String path) {

        return new Path(path);
    }
}
