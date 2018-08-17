package com.hand.hadoop.project;

import com.kumkee.userAgent.UserAgentParser;
import com.kumkee.userAgent.UserAgent;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * UserAgentTest测试类
 */
public class UserAgentTest {
    /*
    单元测试UserAgentParser类使用
     */
    @Test
    public void testUserAgentParser() {
        String source = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36";
        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        String browser = agent.getBrowser();
        String os = agent.getOs();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String platform = agent.getPlatform();
        boolean isMobile = agent.isMobile();

        System.out.println(browser + "," + os + "," + engine + "," + engineVersion + "," + platform + "," + isMobile);

    }

    @Test
    public void testReadFile() throws IOException {
        String path = "/Users/luozhenfei1/学习/BigData/慕课网-10小时入门大数据/access.log";
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));

        String line = "";
        int i = 0;

        Map<String, Integer> browserMap = new HashMap<String, Integer>();

        UserAgentParser userAgentParser = new UserAgentParser();
        while (line != null) {
            line = bufferedReader.readLine();//一次读一行数据
            i++;
            if (StringUtils.isNotBlank(line)) {
                String source = line.substring(getChareacterPosition(line, "\"", 5) + 1, getChareacterPosition(line, "\"", 6));
                UserAgent agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String os = agent.getOs();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String platform = agent.getPlatform();

                Integer browserValue = browserMap.get(browser);
                if (browserMap.get(browser) != null) {
                    browserMap.put(browser, browserMap.get(browser) + 1);
                } else {
                    browserMap.put(browser, 1);
                }
            }
        }
        System.out.println("总共" + i + "行！");

        for (Map.Entry<String, Integer> entry : browserMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    @Test
    public void testGetChareacterPosition() {
        String value = "2017-09-29\t18:13:06\tPOST\t/HyperionPlanning/faces/PlanningCentral?_adf.ctrl-state=16ls04j3rx_35\t200\t0.034\t1571\t192.168.123.112\t\"005M_55Ne641ZbWzLwVK8A0005Co0007yy\"\t-\t/HyperionPlanning/faces/PlanningCentral\n";
        int index = getChareacterPosition(value, "\"", 1);
        int index1 = getChareacterPosition(value, "\"", 2);
        String str = value.substring(index + 1, index1);
        System.out.println(index + "," + index1);
        System.out.println(str);
    }

    /**
     * 获取指定字符串中指定标识的字符串出现的索引位置
     *
     * @param value    字符串的值
     * @param operator 匹配字符
     * @param index    匹配字符的位置
     * @return
     */
    private int getChareacterPosition(String value, String operator, int index) {
        Matcher matcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (matcher.find()) {
            mIdx++;
            if (mIdx == index) {
                break;
            }
        }
        return matcher.start();
    }
}
