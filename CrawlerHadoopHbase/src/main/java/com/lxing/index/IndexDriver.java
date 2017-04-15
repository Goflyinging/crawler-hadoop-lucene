package com.lxing.index;

import com.lxing.domain.LuceneDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lxing on 2017/4/12.
 */
public class IndexDriver extends Configured implements Tool {
    
    public static Logger logger = LoggerFactory.getLogger(IndexDriver.class);
    
    private static Configuration conf = HBaseConfiguration.create();
    
    static {
        conf.addResource("app-config.xml");
    }
    
    public int run(String[] args) throws Exception {
        String cacheArticleTable = "cache" + conf.get("article.table.name");
        String outPath = conf.get("hdfs.index.path");
        Job job = Job.getInstance(conf, "IndexDriver");
        job.setJarByClass(IndexDriver.class);
        // mapper中直接处理数据并写入hbase
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(IndexOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        // map
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false); // don't set to true for MR jobs
        TableMapReduceUtil.initTableMapperJob(cacheArticleTable,
                                              scan,
                                              IndexMapper.class,
                                              ImmutableBytesWritable.class,
                                              LuceneDocumentWritable.class,
                                              job);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : 0;
    }
    
    public static void main(String[] args) {
        try {
            int returnCode = ToolRunner.run(new IndexDriver(), args);
            System.exit(returnCode);
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
    
}
