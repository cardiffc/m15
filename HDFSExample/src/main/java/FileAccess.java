import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class FileAccess
{
    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     * for example, hdfs://localhost:32771
     */
    private FileSystem hdfs;
    private List<String> paths;

    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        hdfs = FileSystem.get(
                new URI(rootPath), configuration
        );
        paths = new ArrayList<>();

    }

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
        if (!isDirectory(path)) {
            String oldContent = read(path);
            String newContent = oldContent + " " + content;
            delete(path);
            OutputStream os = hdfs.create(getPath(path));
            BufferedWriter br = new BufferedWriter(
                    new OutputStreamWriter(os, "UTF-8")
            );
            br.write(newContent);
            br.flush();
            br.close();
        } else {
            System.out.println("This is directory. Append could not be executed!");
        }

    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(getPath(path))));
        return br.readLine();
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
    public List<String> list(String path) throws IOException {
        FileStatus[] fs = hdfs.listStatus(getPath(path));
        for (int i = 0; i < fs.length ; i++) {
            Path tempPath = fs[i].getPath();
            if (isDirectory(tempPath.toString())) {
                if (!paths.contains(tempPath.toString())) {
                    paths.add(tempPath.toString());
                }
                list(tempPath.toString());
            } else {
                if (!paths.contains(tempPath.toString())) {
                    paths.add(fs[i].getPath().toString());
                }
            }
        }
        return paths;
    }

    public void closeFs () throws IOException {hdfs.close();}
    private Path getPath (String path) { return new Path(path); }
}
