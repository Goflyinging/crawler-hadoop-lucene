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

/**
 * @Description:
 * @author: 路星星
 * @version: 1.0
 * @date: 10:02 2017/5/27
 */
public class Index {
    public static Logger logger = LoggerFactory.getLogger(Index.class);

    private IndexDriver indexDriver;//建立索引驱动

    public Index() {
        indexDriver = new IndexDriver();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("app-config.xml");
        conf.addResource("hbase-site.xml");
        String outPath = conf.get("hdfs.index.path");
        String localPath = conf.get("local.index.path");
        String cacheLocalPath = conf.get("localcache.index.path");
        Index index = new Index();
        // 第四步：为获取的文章信息建索引
        int indexCode = ToolRunner.run(index.indexDriver, args);
        if (indexCode == 0) {
            logger.error("索引建立失败！！！");
            return;
        }
        // 第五步：将索引移至本地，并与本地索引合并
        final FileSystem fs = FileSystem.get(conf);
        fs.moveToLocalFile(new Path(outPath),new Path(cacheLocalPath));
        fs.deleteOnExit(new Path(outPath));
        LuceneUtil.mergeIndex(Paths.get(cacheLocalPath),Paths.get(localPath),new IKAnalyzer(true));
        // 第六步：清空中间表，以备下次使用
        HbaseUtil.clearCache(conf);

    }

}
