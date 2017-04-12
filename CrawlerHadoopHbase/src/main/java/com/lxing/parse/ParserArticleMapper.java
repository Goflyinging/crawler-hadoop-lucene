package com.lxing.parse;

import com.lxing.domain.Article;
import com.lxing.fetch.FetchMapper;
import com.lxing.util.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Created by lxing on 2017/4/12.
 */
public class ParserArticleMapper extends TableMapper<ImmutableBytesWritable, Put> {
    public static Logger logger = LoggerFactory.getLogger(FetchMapper.class);
    private String CSDNarticleRegex;
    private String cacheArticleTable;
    private String articleTable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        articleTable = conf.get("article.table.name");
        cacheArticleTable = "cache" + articleTable;
        CSDNarticleRegex = conf.get("csdn.article.regex");
    }

    public void map(ImmutableBytesWritable key, Result values,
                    Context context) throws IOException, InterruptedException {
        byte[] keyBytes = key.get();
        String url = new String(keyBytes);
        String suffix;
        //csdn article  处理csdn博客
        if (url.matches(CSDNarticleRegex)) {
            for (Cell cell : values.rawCells()) {
                if ("document".equals(Bytes.toString(CellUtil.cloneQualifier(cell))) && "info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    String document = Bytes.toString(CellUtil.cloneValue(cell));
                    if (null == document ||document.trim().equals(""))
                        break;
                    logger.info("解析："+url);
                    Article article = Parser.parserCSDNArticle(url, document);
                    if (article==null)
                        break;
                    suffix = "csdn";
                    Put put = getPut(suffix, article);
                    context.write(new ImmutableBytesWritable(articleTable.getBytes()), put);
                    context.write(new ImmutableBytesWritable(cacheArticleTable.getBytes()), put);
                }
            }
        }

    }

    //将信息写入put中
    private Put getPut(String suffix, Article article) {
        String rowkey = article.getId() + "_" + suffix;
        Put put = new Put(rowkey.getBytes());
        put.addColumn("info".getBytes(), "id".getBytes(), article.getId().getBytes());
        put.addColumn("info".getBytes(), "title".getBytes(), article.getTitle().getBytes());
        put.addColumn("info".getBytes(), "author".getBytes(), article.getAuthor().getBytes());
        put.addColumn("info".getBytes(), "id".getBytes(), article.getId().getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), article.getContent().getBytes());
        put.addColumn("info".getBytes(), "url".getBytes(), article.getUrl().getBytes());
        return put;
    }

}