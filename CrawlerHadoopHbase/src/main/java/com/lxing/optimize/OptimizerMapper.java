package com.lxing.optimize;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/11.
 */
public class OptimizerMapper extends TableMapper<ImmutableBytesWritable, BooleanWritable> {
    public static Logger logger = LoggerFactory.getLogger(OptimizerMapper.class);
    BooleanWritable trueflag = new BooleanWritable(true);
    BooleanWritable falseflag = new BooleanWritable(false);

    //查询状态为1的对info.outlinks 进行处理
    public void map(ImmutableBytesWritable key, Result values,
                    Context context) throws IOException, InterruptedException {
        context.write(key, trueflag);
        String status = Bytes.toString(values.getValue("info".getBytes(), "status".getBytes()));
        if ("1".equals(status)){
            String outlinks = Bytes.toString(values.getValue("info".getBytes(), "outlinks".getBytes()));
            logger.info("url：" + Bytes.toString(key.get()));
            String[] urls = outlinks.split(", ");
            for (String url : urls) {
                context.write(new ImmutableBytesWritable(url.getBytes()), falseflag);
            }
        }
    }

}