package com.lxing.fetch;

import com.lxing.util.Crawler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/11.
 */
public class FetchReducer extends
        TableReducer<ImmutableBytesWritable,LongWritable, ImmutableBytesWritable> {
    public static Logger logger = LoggerFactory.getLogger(FetchReducer.class);
    private String saveTable;
    private String cacheSaveTable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        saveTable = conf.get("html.table.name");
        cacheSaveTable = "cache" + saveTable;
    }

    public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {
        logger.info("Start FetchReducer...");
        String url = Bytes.toString(key.get());
        ImmutableBytesWritable putTable1 = new ImmutableBytesWritable(Bytes.toBytes(saveTable));
        ImmutableBytesWritable putTable2 = new ImmutableBytesWritable(Bytes.toBytes(cacheSaveTable));
        if (url != null && !url.equals("")) {
            logger.info("url:" + url);
            String text = Crawler.crawl(url);
            if(text==null)
                return;
            Put put = new Put(key.get());
            put.addColumn("info".getBytes(), "document".getBytes(), text.getBytes());
            context.write(putTable1, put);
            context.write(putTable2, put);
        }
    }

}

