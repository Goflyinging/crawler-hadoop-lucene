package com.lxing.main;

import org.apache.hadoop.util.ToolRunner;

public class Crawler {

//    private CrawlerDriver crawler;
//    private ParserDriver parser;
//    private OptimizerDriver optimizer;
//    private HtmlToXMLDriver xml_convert;
//
//    public Crawler() {
//        crawler = new CrawlerDriver();
//        parser = new ParserDriver();
//        optimizer = new OptimizerDriver();
//        xml_convert = new HtmlToXMLDriver();
//    }
//
//    public static void main(String[] args) throws Exception {
//        Crawler crawleMain = new Crawler();
//        int depth = 1;
//        for (int i = 1; i <= depth; i++) {
//            System.out.println("第 " + i + " 层:开始执行CrawlerDriver,下载页面");
//            int crawlerCode = ToolRunner.run(crawleMain.crawler, args);
//            if (crawlerCode == 1) {
//                System.out.println("第 " + i + " 层:开始执行ParserDriver,分析页面提取URL");
//                int parserCode = ToolRunner.run(crawleMain.parser, args);
//                if (parserCode == 1) {
//                    System.out.println("第 " + i + " 层:开始执行OptimizerDriver,优化URL");
//                    int optimizerCode = ToolRunner.run(crawleMain.optimizer, args);
//                    if (optimizerCode == 1) {
//                        System.out.println("第 " + i + " 层:开始执行HtmlToXMLDriver,将页面转换成XML保存");
//                        int convertCode = ToolRunner.run(crawleMain.xml_convert, args);
//                        if (convertCode == 1) {
//                            System.out.println("第 " + i + " 层：抓取完毕");
//                        }
//                    }
//                }
//            }
//        }
//    }
}
