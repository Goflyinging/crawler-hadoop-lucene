package com.lxing.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lxing.domain.Article;
import org.apache.hadoop.conf.Configuration;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parser {
    private static Logger logger = LoggerFactory.getLogger(Parser.class);
    
    private static Configuration conf = new Configuration();
    
    static {
        conf.addResource("app-config.xml");
    }
    
    public static Set<String> parserUrls(String url, String html) {
        Set<String> urlSet = new HashSet<String>();
        Document doc = Jsoup.parse(html, url);
        String regex = conf.get("url.filter.regex");
        Elements links = doc.select("a[href]");
        logger.info("开始分析网页:" + url + "     Links: " + links.size());
        Pattern pattern = Pattern.compile(regex);
        for (Element link : links) {
            String u = link.attr("abs:href");
            if (pattern.matcher(u).matches()) {
                urlSet.add(u);
            }
        }
        return urlSet;
    }
    
    public static Article parserCSDNArticle(String url, String htmlSource) {
        Article article = new Article();
        // url
        article.setUrl(url);
        // author ID
        String[] splits = url.split("/");
        if (splits.length > 6) {
            article.setAuthor(splits[3]);
            article.setId(splits[6]);
        }
        else {
            System.out.println("错误：作者和ID解析失败------");
        }
        Document doc = Jsoup.parse(htmlSource);
        // title
        Elements titleElements = doc.select("div.article_title");
        // csdn第二种页面风格
        if (titleElements.first() == null) {
            // date
            Elements elements = doc.select("div.date");
            elements = elements.first().children();
            // 2017 四月
            Element element = elements.first();
            String year = element.children().first().text();
            String month = element.children().last().text();
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
            String day = elements.last().text();
            article.setDate(year + "-" + month + "-" + day);
            // title
            element = doc.select("h3.list_c_t").first();
            if (element == null) {
                return null;
            }
            element = element.children().first();
            article.setTitle(element.text());
            // content
            elements = doc.select("div.skin_detail");
            if (elements != null) {
                article.setContent(elements.first().text());
            }
        }
        else {
            Element titleE = titleElements.first().select("a").first();
            if (null == titleE)
                return null;
            article.setTitle(titleE.text());
            // date
            Element dateE = doc.select("span.link_postdate").first();
            if (null != dateE)
                article.setDate(dateE.html());
            
            // content
            Element contentE = doc.select("div.article_content").first();
            if (null != contentE) {
                String content = contentE.text().replaceAll("\\s*", "");
                article.setContent(content);
            }
        }
        if (article.getContent().isEmpty())
            article.setContent("空文章");
        return article;
    }
    
}
