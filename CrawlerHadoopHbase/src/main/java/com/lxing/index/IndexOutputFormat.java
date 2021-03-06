package com.lxing.index;

import com.lxing.domain.LuceneDocumentWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;

public class IndexOutputFormat extends
                               FileOutputFormat<ImmutableBytesWritable, LuceneDocumentWritable> {
    
    public static Logger logger =
                                LoggerFactory.getLogger(IndexOutputFormat.class);
    
    private Random random = new Random();
    
    public RecordWriter<ImmutableBytesWritable, LuceneDocumentWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
                                                                                                                    InterruptedException {
        final Path outputPath = getDefaultWorkFile(context, "");
        Configuration conf = context.getConfiguration();
        final FileSystem fs = FileSystem.get(conf);
        final java.nio.file.Path localPath =
                                           Paths.get("/indexTemp/_"
                                                     + Integer.toString(random.nextInt()));
        // 删除原来建立的索引
        File file = localPath.toFile();
        if (file.exists()) {
            logger.error("文件夹已经存在，请删除！！！");
            System.exit(0);
        }
        final Directory indexDir = FSDirectory.open(localPath);
        Analyzer analyzer = new IKAnalyzer(true); // 新建一个分词器实例
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        final IndexWriter writer = new IndexWriter(indexDir, config);// 构造一个索引写入器
        
        return new RecordWriter<ImmutableBytesWritable, LuceneDocumentWritable>() {
            
            @Override
            public void write(ImmutableBytesWritable key,
                              LuceneDocumentWritable value) throws IOException,
                                                            InterruptedException {
                Document doc = value.get();
                writer.addDocument(doc);
            }
            
            @Override
            public void close(TaskAttemptContext context) throws IOException,
                                                          InterruptedException {
                writer.forceMerge(1);
                writer.close();
                indexDir.close();
                logger.info("localPath:" + localPath.toString());
                logger.info("outputPath:" + outputPath.toString());
                fs.moveFromLocalFile(new Path(localPath.toString()),
                                     outputPath);
            }
            
        };
    }
}
