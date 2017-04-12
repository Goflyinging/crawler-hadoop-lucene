package com.lxing.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lxing on 2017/4/12.
 * 解析网页URL地址
 */
public class ParserUrlDriver extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(com.lxing.parse.ParserUrlDriver.class);
    private static Configuration conf = HBaseConfiguration.create();

    static {
        conf.addResource("app-config.xml");
    }

    public int run(String[] args) throws Exception {
        String cacheSaveTable = "cache"+conf.get("html.table.name");
        String urlTableName = conf.get("url.table.name");
        Job job = Job.getInstance(conf, "ParserUrlDriver");
        job.setJarByClass(ParserUrlDriver.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        //mapper中直接处理数据并写入hbase
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,urlTableName);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setNumReduceTasks(0);
        //map
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(cacheSaveTable, scan,
                ParserUrlMapper.class, ImmutableBytesWritable.class, Put.class, job);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : 0;
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ParserUrlDriver.class);
        try {
            int returnCode = ToolRunner.run(new ParserUrlDriver(), args);
            System.exit(returnCode);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}