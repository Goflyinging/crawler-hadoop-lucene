package parse;

import com.lxing.domain.Article;
import com.lxing.util.Crawler;
import com.lxing.util.Parser;
import org.junit.Test;

import java.util.HashSet;

/**
 * Created by lxing on 2017/4/12.
 */
public class parser {

    @Test
    public void testArticelPaser() {
        String url = "http://blog.csdn.net/wgyscsf/article/details/55107525";
        String url1 = "http://blog.csdn.net/anprou/article/details/70145367";
        String html = Crawler.crawl(url);
        Article article = Parser.parserCSDNArticle(url, html);
        System.out.println(article);
        html = Crawler.crawl(url1);
        article = Parser.parserCSDNArticle(url, html);
        System.out.println(article);
    }

    @Test
    public void testurlPaser() {
        String url = " http://blog.csdn.net";
        //前面不可以出现空格
        String html = Crawler.crawl(url);
        System.out.println(html);

    }

    @Test
    public void testSet() {
        HashSet<String> set = new HashSet<>();
        set.add("111111111");
        set.add("222222222");
        set.add("333333333");
        System.out.println(set.toString());
        String[] urls = set.toString().split(", ");
        for (String url : urls) {
            System.out.println(url);
        }
    }

    @Test
    public void testMonth() {
        String month = "一月";
        switch (month.substring(0, month.length() - 1)) {
            case "一":
                month = "01";
                break;
            case "二":
                month = "02";
                break;
            case "三":
                month = "03";
                break;
            case "四":
                month = "04";
                break;
            case "五":
                month = "05";
                break;
            case "六":
                month = "06";
                break;
            case "七":
                month = "07";
                break;
            case "八":
                month = "08";
                break;
            case "九":
                month = "09";
                break;
            case "十":
                month = "10";
                break;
            case "十一":
                month = "11";
                break;
            case "十二":
                month = "12";
                break;
        }
        System.out.print(month);
    }


}
