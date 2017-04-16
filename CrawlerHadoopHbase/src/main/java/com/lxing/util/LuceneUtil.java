package com.lxing.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by lxing on 2017/4/15.
 */
public class LuceneUtil {
    public static void mergeIndex(Path from, Path to, Analyzer analyzer) {
        IndexWriter indexWriter = null;
        try {
            final Directory indexDir = FSDirectory.open(to);
            IndexWriterConfig indexWriterConfig =
                                                new IndexWriterConfig(analyzer);
            System.out.println("正在合并索引文件!\t ");
            indexWriter = new IndexWriter(indexDir, indexWriterConfig);
            File file = from.toFile();
            File[] f = file.listFiles();
            for (File f1 : f) {
                if (f1.isDirectory()) {
                    Directory fromDir = FSDirectory.open(f1.toPath());
                    indexWriter.addIndexes(fromDir);
                    File[] f2 = f1.listFiles();
                    for (File f3 : f2) {
                        f3.delete();
                    }
                    f1.delete();
                    
                }
                else {
                    f1.delete();
                }
            }
            file.delete();
            // Directory fromDir = FSDirectory.open(from);
            // indexWriter.addIndexes(fromDir);
            indexWriter.forceMerge(1);
            indexWriter.close();
            System.out.println("已完成合并!\t ");
        }
        catch (Exception e) {
            System.out.println("合并索引出错！");
            e.printStackTrace();
        }
        finally {
            try {
                if (indexWriter != null)
                    indexWriter.close();
            }
            catch (Exception e) {
                
            }
            
        }
        
    }
    
    public static void main(String[] areg) {
        Path from = Paths.get("");
        Path to = Paths.get("");
        mergeIndex(from, to, new IKAnalyzer(true));
    }
    
}
