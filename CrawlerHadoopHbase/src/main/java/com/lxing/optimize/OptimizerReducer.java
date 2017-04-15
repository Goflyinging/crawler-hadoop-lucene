package com.lxing.optimize;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class OptimizerReducer extends
                              TableReducer<ImmutableBytesWritable, BooleanWritable, ImmutableBytesWritable> {
    
    public static Logger logger =
                                LoggerFactory.getLogger(OptimizerReducer.class);
    
    /***
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(ImmutableBytesWritable key,
                          Iterable<BooleanWritable> values,
                          Context context) throws IOException,
                                           InterruptedException {
        Iterator<BooleanWritable> v = values.iterator();
        if (key.getLength() == 0) {
            return;
        }
        boolean flag = false;
        while (v.hasNext()) {
            BooleanWritable booleanWritable = v.next();
            if (booleanWritable.get() == true) {
                flag = true;
                break;
            }
        }
        byte[] bytes = key.copyBytes();
        Put put = new Put(bytes);
        if (flag == false) {
            logger.info("添加url：" + new String(bytes));
            put.addColumn("info".getBytes(),
                          "status".getBytes(),
                          "0".getBytes());
            context.write(key, put);
        }
        else {
            put.addColumn("info".getBytes(),
                          "status".getBytes(),
                          "3".getBytes());
            context.write(key, put);
        }
        
    }
}
