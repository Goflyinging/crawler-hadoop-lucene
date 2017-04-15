package com.lxing.main;

import com.lxing.fetch.FetchDriver;
import com.lxing.index.IndexDriver;
import com.lxing.inject.InjectDriver;
import com.lxing.optimize.OptimizerDriver;
import com.lxing.parse.ParserArticleDriver;
import com.lxing.parse.ParserUrlDriver;
import com.lxing.util.HbaseUtil;
import com.lxing.util.LuceneUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.nio.file.Paths;

public class Crawler {
    
    public static Logger logger = LoggerFactory.getLogger(InjectDriver.class);
    
    private InjectDriver injectDriver;//注入驱动
    
    private FetchDriver fetchDriver;//爬取驱动
    
    private ParserUrlDriver parserUrlDriver;//解析URL驱动
    
    private OptimizerDriver optimizerDriver;//去重url驱动
    
    private ParserArticleDriver parserArticleDriver;//解析文章驱动
    
    private IndexDriver indexDriver;//建立索引驱动
    
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
        String outPath = conf.get("hdfs.index.path");
        String localPath = conf.get("local.index.path");
        String cacheLocalPath = conf.get("localcache.index.path");
        Crawler crawleMain = new Crawler();
        int depth = 2;
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
        // 第五步：清空中间表，以备下次使用
        HbaseUtil.clearCache(conf);
        // 第六步：将索引移至本地，并与本地索引合并
        final FileSystem fs = FileSystem.get(conf);
        fs.moveToLocalFile(new Path(outPath),new Path(cacheLocalPath));
        fs.deleteOnExit(new Path(outPath));
        LuceneUtil.mergeIndex(Paths.get(cacheLocalPath),Paths.get(localPath),new IKAnalyzer(true));
        
    }
}
