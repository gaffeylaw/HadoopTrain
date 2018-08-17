package com.hand.hadoop.access;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by v_lvyuan on 2017/7/12.
 */
public class AccessLog {
    public static String getOne(String[] array) {
        int length = array.length;
        Random r = new Random();
        int rand = r.nextInt(length);
        return array[rand] + " ";
    }

    public static void main(String[] args) throws IOException {
        String[] str = {"-"};
        String[] remote_add = {"101.201.252.98", "101.231.252.98", "101.201.200.11", "103.201.200.11",
                "110.201.200.11", "113.201.200.11", "115.201.200.11", "115.201.200.111","119.201.201.111",
                "120.201.201.111","101.231.250.98","101.231.222.98","101.230.222.98","111.230.222.98",
                "121.201.201.111","121.201.201.123","101.231.251.98","222.222.222.222"};
        String[] rt_time = {"21", "43", "67", "599", "12", "760", "230", "120", "455", "78", "98"};
        String[] time = {"[09/Apr/2017:06:34:01 +0800]", "[09/Apr/2017:16:43:45 +0800]", "[09/Apr/2017:08:30:19 +0800]",
                "[09/Apr/2017:18:30:29 +0800]", "[09/Apr/2017:09:31:39 +0800]", "[09/Apr/2017:16:41:22 +0800]",
                "[09/Apr/2017:15:15:33 +0800]", "[09/Apr/2017:9:00:00 +0800]", "[10/Apr/2017:12:00:00 +0800]"};
        String[] request_method = {"GET", "POST"};
        String[] request_url = {"\"http://www.imooc.com/learn/986\"", "\"http://www.imooc.com/code/123\"",
                "\"http://www.imooc.com/learn/814\"", "\"http://www.imooc.com/code/122\"",
                "\"http://www.imooc.com/article/27544\"", "\"http://www.imooc.com/article/223\"",
                "\"http://www.imooc.com/learn/391\"", "\"http://www.imooc.com/video/14388\"",
                "\"http://www.imooc.com/video/7646\"","\"http://www.imooc.com/video/2455\"",
                "\"http://www.imooc.com/article/4321\"","\"http://www.imooc.com/video/8273\""};
        String[] refer = {"http://www.google.com", "http://www.imooc.com", "http://www.baidu.com", "http://www.taobao.com",
                "http://www.qq.com", "http://www.sina.com"};
        String[] status = {"\"200\"", "\"301\"", "\"302\"", "\"404\"", "\"500\""};
        String[] send_bytes = {"3344", "2789", "490", "439274", "80834", "31232", "432004", "49397", "98324",
                "48243", "432943", "432943"};
        File file = new File("/Users/luozhenfei1/IdeaProjects/SparkSQLProject/data/access.log");
        for (int i = 0; i < 100000; i++) {
            String line = getOne(remote_add) + getOne(str) + getOne(str) + getOne(time) + getOne(request_method) + getOne(request_url)
                    + getOne(status) + getOne(send_bytes);
            ArrayList<String> lines = new ArrayList<String>();
            lines.add(line);
            FileUtils.writeLines(file, lines, true);
            //System.out.println(line);
        }
    }
}
