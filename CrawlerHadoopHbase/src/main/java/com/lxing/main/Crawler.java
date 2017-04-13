package com.lxing.main;

import com.lxing.fetch.FetchDriver;
import com.lxing.inject.InjectDriver;
import com.lxing.optimize.OptimizerDriver;
import com.lxing.parse.ParserUrlDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

public class Crawler {
    private InjectDriver injectDriver;
    private FetchDriver fetchDriver;
    private ParserUrlDriver parserUrlDriver;
    private OptimizerDriver optimizerDriver;


    public Crawler() {
        injectDriver = new InjectDriver();
        fetchDriver = new FetchDriver();
        parserUrlDriver = new ParserUrlDriver();
        optimizerDriver = new OptimizerDriver();
    }

    //
//    public Crawler() {
//        crawler = new CrawlerDriver();
//        parser = new ParserDriver();
//        optimizer = new OptimizerDriver();
//        xml_convert = new HtmlToXMLDriver();
//    }
//
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("app-config.xml");
        Crawler crawleMain = new Crawler();
        int depth = 4;
        int in = crawleMain.injectDriver.injectFromLocal(conf);
        if (in == 0)
            return;
        for (int i = 1; i <= depth; i++) {
            System.out.println("第 " + i + " 层:开始执行CrawlerDriver,下载页面");
            int fetchCode = ToolRunner.run(crawleMain.fetchDriver, args);
            if (fetchCode == 0)
                return;
            System.out.println("第 " + i + " 层:开始执行ParserDriver,分析页面提取URL");
            int parserUrlCode = ToolRunner.run(crawleMain.parserUrlDriver, args);
            if (parserUrlCode == 0)
                return;
            System.out.println("第 " + i + " 层:开始执行OptimizerDriver,优化URL");
            int optimizerCode = ToolRunner.run(crawleMain.optimizerDriver, args);
            if (optimizerCode == 0)
                return;
            System.out.println("第 " + i + " 层：抓取完毕");
        }
    }
}
