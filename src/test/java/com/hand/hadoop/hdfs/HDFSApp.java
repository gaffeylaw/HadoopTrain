package com.hand.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://hadoop01:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/a.txt"));
        fsDataOutputStream.write("hello hadoop!\n".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 查看HDFS文件内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hdfsapi/a.txt"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
        fsDataInputStream.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/a.txt");
        Path newPath = new Path("/hdfsapi/b.txt");
        fileSystem.rename(oldPath, newPath);

    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void copyFromLocaFile() throws Exception {
        Path localPath = new Path("/Users/luozhenfei1/学习/BigData/慕课网-10小时入门大数据/note/3");
        Path hdfsPath = new Path("/hdfsapi");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传文件到HDFS带进度条
     */
    @Test
    public void copyFromLocaFileWithProgress() throws Exception {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("/Users/luozhenfei1/学习/BigData/hadoop/hadoop-2.7.1.tar.gz")));
        OutputStream outputStream = fileSystem.create(new Path("/hdfsapi/hadoop-2.7.1.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");//带进度条提示
            }
        });
        IOUtils.copyBytes(inputStream, outputStream, 4096);
    }

    /**
     * 下载HDFS
     */
    @Test
    public void copyToLocaFile() throws Exception {
        Path localPath = new Path("/Users/luozhenfei1/学习/BigData/慕课网-10小时入门大数据/");
        Path hdfsPath = new Path("/hdfsapi/3");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t\t" + replication + "\t\t" + len + "\t\t" + path);
        }
    }

    /**
     * 删除
     */
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/hadoop-2.7.1.tar.gz"));
        listFiles();
    }

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem.close();
        System.out.println("HDFSApp tearDown");
    }
}
