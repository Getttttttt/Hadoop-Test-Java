import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;

public class HDFSCleanUp {

    private static final String HDFS_URI = "hdfs://localhost:9000";

    // 删除文件或目录
    public static void deletePath(String pathStr) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), configuration);
        Path path = new Path(pathStr);
        if (fs.exists(path)) {
            fs.delete(path, true); // true 表示递归删除
            System.out.println("Deleted: " + pathStr);
        } else {
            System.out.println("Path does not exist: " + pathStr);
        }
        fs.close();
    }

    public static void main(String[] args) {
        try {
            // 逆序删除创建的文件和目录
            deletePath("/user/hadoop/test1/test.txt");
            deletePath("/user/hadoop/test1");
            deletePath("/user/hadoop/test");
            // 注意: 如果/test目录下有其他文件或目录，上面的递归删除会将其一并删除
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
