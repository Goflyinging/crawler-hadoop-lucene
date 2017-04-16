package com.lxing.fetch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FetchDriver extends Configured implements Tool {
    
    public static Logger logger = LoggerFactory.getLogger(FetchDriver.class);
    
    private static Configuration conf = HBaseConfiguration.create();
    
    static {
        conf.addResource("app-config.xml");
    }
    
    public int run(String[] args) throws ClassNotFoundException,
                                  IOException,
                                  InterruptedException {
        String urlTableName = conf.get("url.table.name");
        Job job = Job.getInstance(conf, "FetchDriver");
        job.setJarByClass(FetchDriver.class);
        job.setReducerClass(FetchReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(urlTableName,
                                              scan,
                                              FetchMapper.class,
                                              ImmutableBytesWritable.class,
                                              LongWritable.class,
                                              job);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : 0;
    }
    
    public static void main(String[] args) throws IOException,
                                           InterruptedException {
        try {
            int returnCode = ToolRunner.run(new FetchDriver(), args);
            System.exit(returnCode);
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
    
}
