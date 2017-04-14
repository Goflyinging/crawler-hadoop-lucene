package com.lxing.index;

import com.lxing.domain.LuceneDocumentWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/11.
 */
public class IndexMapper extends TableMapper<ImmutableBytesWritable, LuceneDocumentWritable> {
    public static Logger logger = LoggerFactory.getLogger(IndexMapper.class);

    public void map(ImmutableBytesWritable key, Result values,
                    Context context) throws IOException, InterruptedException {
        Document doc = new Document();
        String rowKey = new String(key.copyBytes());
        logger.info(rowKey);
        String author = Bytes.toString(values.getValue("info".getBytes(), "author".getBytes()));
        String title = Bytes.toString(values.getValue("info".getBytes(), "title".getBytes()));
        String content = Bytes.toString(values.getValue("info".getBytes(), "content".getBytes()));
        //rowkey只存储
        Field keyField = new StoredField("rowkey", rowKey);
        doc.add(keyField);
        //author 只索引 不分词 不存储
        if (author != null || !author.trim().equals("")) {
            Field authorField = new StringField("author", author, Field.Store.NO);
            doc.add(authorField);
        }
        //title content索引 分词 不存储
        if (title != null || !title.trim().equals("")) {
            Field titleField = new TextField("title", title, Field.Store.NO);
            doc.add(titleField);
        }
        if (content != null || !content.equals("")) {
            Field contentField = new TextField("content", content, Field.Store.NO);
            doc.add(contentField);
        }
        context.write(key, new LuceneDocumentWritable(doc));
    }


}