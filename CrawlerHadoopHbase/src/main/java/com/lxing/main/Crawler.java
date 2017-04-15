package com.lxing.main;

import com.lxing.fetch.FetchDriver;
import com.lxing.index.IndexDriver;
import com.lxing.inject.InjectDriver;
import com.lxing.optimize.OptimizerDriver;
import com.lxing.parse.ParserArticleDriver;
import com.lxing.parse.ParserUrlDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crawler {
    
    public static Logger logger = LoggerFactory.getLogger(InjectDriver.class);
    
    private InjectDriver injectDriver;
    
    private FetchDriver fetchDriver;
    
    private ParserUrlDriver parserUrlDriver;
    
    private OptimizerDriver optimizerDriver;
    
    private ParserArticleDriver parserArticleDriver;
    
    private IndexDriver indexDriver;
    
    public Crawler() {
        injectDriver = new InjectDriver();
        fetchDriver = new FetchDriver();
        parserUrlDriver = new ParserUrlDriver();
        optimizerDriver = new OptimizerDriver();
        parserArticleDriver = new ParserArticleDriver();
        indexDriver = new IndexDriver();
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("app-config.xml");
        Crawler crawleMain = new Crawler();
        int depth = 4;
        // 第一步： 注入待抓取的网页url
        int in = crawleMain.injectDriver.injectFromLocal(conf);
        if (in == 0) {
            logger.error("注入待抓取的url失败！！");
            return;
        }
        // 第二步：循环抓取网页并解析网页上的url，根据正则表达式获取需要的url，并进行去重处理
        for (int i = 1; i <= depth; i++) {
            System.out.println("第 " + i + " 层:开始执行CrawlerDriver,下载页面");
            int fetchCode = ToolRunner.run(crawleMain.fetchDriver, args);
            if (fetchCode == 0) {
                logger.error("第" + i + "次抓取网页失败！！");
                return;
            }
            System.out.println("第 " + i + " 层:开始执行parserUrlDriver,分析页面提取URL");
            int parserUrlCode =
                              ToolRunner.run(crawleMain.parserUrlDriver, args);
            if (parserUrlCode == 0) {
                logger.error("第" + i + "次解析网页url失败！！");
                return;
            }
            System.out.println("第 " + i + " 层:开始执行OptimizerDriver,优化URL");
            int optimizerCode =
                              ToolRunner.run(crawleMain.optimizerDriver, args);
            if (optimizerCode == 0) {
                logger.error("第" + i + "次优化网页url失败！！");
                return;
            }
            System.out.println("第 " + i + " 层：抓取完毕");
        }
        // 第三步：解析已抓取的网页信息，获取文章内容并存储
        int parserArticleCode = ToolRunner.run(crawleMain.parserArticleDriver,
                                               args);
        if (parserArticleCode == 0) {
            logger.error("解析网页文章信息失败！！！");
            return;
        }
        // 第四步：为获取的文章信息建索引
        int indexCode = ToolRunner.run(crawleMain.indexDriver, args);
        if (indexCode == 0) {
            logger.error("索引建立失败！！！");
            return;
        }
        
    }
}
