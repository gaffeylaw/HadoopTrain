package com.hand.hadoop.spring;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * 使用Spring Boot的方式访问HDFS
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {
    @Autowired
    FsShell fsShell;//使用FsShell来操作

    public void run(String... strings) {
        for (FileStatus fileStatus : fsShell.lsr("/springhdfs")) {
            System.out.println("> " + fileStatus.getPath());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHDFSApp.class, args);
    }

}
