package com.lxing.parse;

import com.lxing.fetch.FetchMapper;
import com.lxing.util.Parser;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Created by lxing on 2017/4/12.
 */
public class ParserUrlMapper extends TableMapper<ImmutableBytesWritable, Put> {
    public static Logger logger = LoggerFactory.getLogger(FetchMapper.class);
    private Text rowKey = new Text();
    public void map(ImmutableBytesWritable key, Result values,
                    Context context) throws IOException, InterruptedException {
        byte[] keyBytes = key.get();
        rowKey.set(Bytes.toString(keyBytes));
        for (Cell cell : values.rawCells()) {
            if ("document".equals(Bytes.toString(CellUtil.cloneQualifier(cell))) && "info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                //开始读取网页信息
                String document = Bytes.toString(CellUtil.cloneValue(cell));
                Set set = Parser.parserUrls(rowKey.toString(), document);
                //去除[]
                String urls = set.toString();
                String outlinks = urls.substring(1,urls.length()-1);
                logger.info(rowKey.toString()+": outlinks\n" +outlinks);
                Put put = new Put(keyBytes);
                //info:outlinks
                put.addColumn("info".getBytes(), "outlinks".getBytes(), outlinks.getBytes());
                //info:status
                put.addColumn("info".getBytes(), "status".getBytes(), "1".getBytes());
                context.write(key,put);
            }
        }
    }

}