package parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/12.
 */
public class regex {
    String webfilter = null;
    String csdnfilter = null;

    @Before
    public void setup() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("app-config.xml");
        webfilter = conf.get("url.filter.regex");
        csdnfilter = conf.get("csdn.article.regex");
    }

    @Test
    public  void urlRegexTest() {
        String url = "http://blog.csdn.net";
        String url2 = "http://my.csdn.net/my/mycsdn?list=1&list2=1";
        String reg1 = "^((http|https)://)?((blog|my)\\.csdn\\.net)(/[(\\w)-./?%&=]*)?$";
        System.out.println("webfilter: " + webfilter);
        System.out.println("reg1: " + reg1);
        System.out.println(url.matches(reg1));
        System.out.println(url2.matches(reg1));
        System.out.println(url.matches(webfilter));
        System.out.println(url2.matches(webfilter));
    }
    @Test
    public  void csdnRegexTest() {
        String url = "http://blog.csdn.net";
        String url2 = "http://blog.csdn.net/tanggao1314/article/details/51382382";
        System.out.println("csdnfilter: " + csdnfilter);
        System.out.println(url.matches(csdnfilter));
        System.out.println(url2.matches(csdnfilter));
    }

}
