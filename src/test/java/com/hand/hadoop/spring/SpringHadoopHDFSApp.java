package com.hand.hadoop.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * 使用Spring Hadoop 依赖注入的特点来访问HDFS文件系统
 */
public class SpringHadoopHDFSApp {
    private ApplicationContext ctx;
    private FileSystem fileSystem;

    /**
     * 创建文件夹
     *
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception {
        fileSystem.mkdirs(new Path("/springhdfs"));
    }

    /**
     * 查看HDFS文件内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
        fsDataInputStream.close();
    }

    /**
     * 其他方法和HDFSApp的写法一致，只是利用了Spring的依赖注入方式
     */

    @Before
    public void setUp() {
        ctx = new ClassPathXmlApplicationContext("context.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
    }

    @After
    public void tearDown() throws IOException {
        ctx = null;
        fileSystem.close();
    }
}
