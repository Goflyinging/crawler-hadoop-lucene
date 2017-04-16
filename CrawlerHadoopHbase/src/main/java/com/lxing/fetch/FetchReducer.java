package com.lxing.fetch;

import com.lxing.util.Crawler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FetchReducer extends
                          TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {
    
    public static Logger logger = LoggerFactory.getLogger(FetchReducer.class);
    
    private String saveTable;
    
    private String cacheSaveTable;
    
    protected void setup(Context context) throws IOException,
                                          InterruptedException {
        Configuration conf = context.getConfiguration();
        saveTable = conf.get("html.table.name");
        cacheSaveTable = "cache" + saveTable;
    }
    
    public void reduce(ImmutableBytesWritable key,
                       Iterable<LongWritable> values,
                       Context context) throws IOException,
                                        InterruptedException {
        System.out.println("Start FetchReducer...");
        logger.info("Start FetchReducer...");
        String url = Bytes.toString(key.copyBytes());
        ImmutableBytesWritable putTable1 =
                                         new ImmutableBytesWritable(saveTable.getBytes());
        ImmutableBytesWritable putTable2 =
                                         new ImmutableBytesWritable(cacheSaveTable.getBytes());
        if (url != null && !url.equals("")) {
            System.out.println("url:" + url);
            logger.info("url:" + url);
            String text = Crawler.crawl(url);
            // String text=null;
            System.out.println("text:" + url);
            if (text != null) {
                Put put = new Put(key.get());
                put.addColumn("info".getBytes(),
                              "document".getBytes(),
                              text.getBytes());
                context.write(putTable1, put);
                context.write(putTable2, put);
            }
        }
    }
    
}
