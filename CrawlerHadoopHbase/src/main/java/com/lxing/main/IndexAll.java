package com.lxing.main;

import java.nio.file.Paths;

import com.lxing.index.IndexAllDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.lxing.index.IndexDriver;
import com.lxing.util.HbaseUtil;
import com.lxing.util.LuceneUtil;

/**
 * @Description:
 * @author: 路星星
 * @version: 1.0
 * @date: 10:02 2017/5/27
 */
public class IndexAll {
    public static Logger logger = LoggerFactory.getLogger(IndexAll.class);

    private IndexAllDriver indexAllDriver;//建立索引驱动

    public IndexAll() {
        indexAllDriver = new IndexAllDriver();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("app-config.xml");
        conf.addResource("hbase-site.xml");
        String outPath = conf.get("hdfs.index.path");
        String localPath = conf.get("local.index.path");
        String cacheLocalPath = conf.get("localcache.index.path");
        IndexAll index = new IndexAll();
        // 第四步：为获取的文章信息建索引
        int indexCode = ToolRunner.run(index.indexAllDriver, args);
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
