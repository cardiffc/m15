import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
    public FileAccess(String rootPath) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        hdfs = FileSystem.get(
                new URI(rootPath), configuration
        );

    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException {
        Path dir = new Path(path);
        hdfs.mkdirs(dir);
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content)
    {

    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path)
    {

        return null;
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path)
    {

    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        Path checkingPath = new Path(path);
        return hdfs.isDirectory(checkingPath);
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path)
    {
        return null;
    }
}
