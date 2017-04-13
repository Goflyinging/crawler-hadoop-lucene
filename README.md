# crawler-hadoop-lucene
利用hadoop的mapreduce和Hbase，基于lucene做的简单的搜索引擎
## 基本介绍
- InjectDriver 将本地的url注入到hbase数据库中等待下一步执行
- FetchDriver  负责抓取url对应的网页内容
- ParserUrlDriver 解析所抓取网页内容的所有url，并过滤掉不需要的url
- ParserArticleDriver 解析对应url网页内容上的文章信息，以CSDN为例
- OptimizerDriver 将解析的url与已经抓取的url做对比，去掉重复的url并加入，等待下次抓取
