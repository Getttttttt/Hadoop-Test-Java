import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private static final String HDFS_URI = "hdfs://localhost:9000";

    // 创建目录
    public static void mkDir(String directoryPath) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), configuration);
        Path path = new Path(directoryPath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Directory created: " + directoryPath);
        } else {
            System.out.println("Directory already exists: " + directoryPath);
        }
        fs.close();
    }

    // 创建文件
    public static void createFile(String filePath) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), configuration);
        Path path = new Path(filePath);
        if (!fs.exists(path.getParent())) {
            fs.mkdirs(path.getParent());
        }
        if (fs.createNewFile(path)) {
            System.out.println("File created: " + filePath);
        } else {
            System.out.println("File already exists: " + filePath);
        }
        fs.close();
    }

    // 删除文件
    public static void delFile(String filePath) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), configuration);
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            fs.delete(path, false);
            System.out.println("File deleted: " + filePath);
        } else {
            System.out.println("File does not exist: " + filePath);
        }
        fs.close();
    }

    // 批量创建文件并读取元信息
    public static void batchCreateReadFiles(String directoryPath) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), configuration);
        Path dirPath = new Path(directoryPath);
        if (!fs.exists(dirPath)) {
            fs.mkdirs(dirPath);
        }

        long endTime = System.currentTimeMillis() + 60000; // 运行1分钟
        while (System.currentTimeMillis() < endTime) {
            String fileName = "file_" + System.currentTimeMillis() + ".log";
            Path filePath = new Path(directoryPath + "/" + fileName);
            fs.create(filePath).close();
            System.out.println("Created: " + filePath);
            Thread.sleep(10000); // 每10秒创建一个文件
        }

        FileStatus[] fileStatuses = fs.listStatus(dirPath);
        for (FileStatus status : fileStatuses) {
            System.out.println("File: " + status.getPath());
            System.out.println("Permission: " + status.getPermission());
            System.out.println("Size: " + status.getLen());
            System.out.println("Creation time: " + status.getModificationTime());
        }
        fs.close();
    }

    public static void main(String[] args) {
        try {
            mkDir("/user/hadoop/test");
            createFile("/user/hadoop/test1/test.txt");
            delFile("/user/hadoop/test1/test.txt");
            batchCreateReadFiles("/user/hadoop/test");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
