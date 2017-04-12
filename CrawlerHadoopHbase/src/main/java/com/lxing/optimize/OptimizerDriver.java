package com.lxing.optimize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/11.
 * url 去重
 * 将状态为1的 即刚抓取的网页的outlinks 和原来的网页比对并去重
 * 如果为false 即表明没有抓取过 存入表中等待下次抓取
 */
public class OptimizerDriver extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(OptimizerDriver.class);

    private static Configuration conf = HBaseConfiguration.create();

    static {
        conf.addResource("app-config.xml");
    }


    public int run(String[] args) throws ClassNotFoundException, IOException,
            InterruptedException {
        String urlTableName = conf.get("url.table.name");
        Job job = Job.getInstance(conf, "OptimizerDriver");
        job.setJarByClass(OptimizerDriver.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(urlTableName, scan,
                OptimizerMapper.class, ImmutableBytesWritable.class, BooleanWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(urlTableName, OptimizerReducer.class,
                job);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 1 : 0;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {
        try {
            int returnCode = ToolRunner.run(new OptimizerDriver(), args);
            System.exit(returnCode);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
