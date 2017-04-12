package com.lxing.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lxing on 2017/4/12.
 */
public class ParserArticleDriver extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(ParserArticleDriver.class);
    private static Configuration conf = HBaseConfiguration.create();

    static {
        conf.addResource("app-config.xml");
    }

    public int run(String[] args) throws Exception {
        String cacheSaveTable = "cache" + conf.get("html.table.name");
        Job job = Job.getInstance(conf, "ParserArticleDriver");
        job.setJarByClass(ParserArticleDriver.class);
        //mapper中直接处理数据并写入hbase
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setNumReduceTasks(0);
        //map
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false); // don't set to true for MR jobs
        TableMapReduceUtil.initTableMapperJob(cacheSaveTable, scan,
                ParserArticleMapper.class, ImmutableBytesWritable.class, Put.class, job);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : 0;
    }

    public static void main(String[] args) {
        try {
            int returnCode = ToolRunner.run(new ParserArticleDriver(), args);
            System.exit(returnCode);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}