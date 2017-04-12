package com.lxing.index;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class IndexOutputFormat extends FileOutputFormat<ImmutableBytesWritable, LuceneDocumentWrapper> {
	static final Log LOG = LogFactory.getLog(IndexOutputFormat.class);

	private Random random = new Random();

	@Override
	public RecordWriter<ImmutableBytesWritable, LuceneDocumentWrapper> getRecordWriter(final FileSystem fs, JobConf job,
			String name, final Progressable progress) throws IOException {

		final Path perm = new Path(FileOutputFormat.getOutputPath(job), name);
		final Path temp = job.getLocalPath("index/_" + Integer.toString(random.nextInt()));

		LOG.info("To index into " + perm);

		// delete old, if any
		fs.delete(perm, true);

		final IndexConfiguration indexConf = new IndexConfiguration();
		String content = job.get("hbase.index.conf");
		if (content != null) {
			indexConf.addFromXML(content);
		}

		String analyzerName = indexConf.getAnalyzerName();
		Analyzer analyzer;
		try {
			Class<? extends Analyzer> analyzerClass = Class.forName(analyzerName).asSubclass(Analyzer.class);
			Constructor<? extends Analyzer> analyzerCtor = analyzerClass.getConstructor();
			analyzer = analyzerCtor.newInstance();
		} catch (Exception e) {
			throw new IOException("Error in creating an analyzer object " + analyzerName);
		}

		// build locally first
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		java.nio.file.Path path = Paths.get(fs.startLocalOutput(perm, temp).toString());
		
		iwc.setUseCompoundFile(indexConf.isUseCompoundFile());
		iwc.setMaxBufferedDocs(indexConf.getMaxBufferedDocs());
		LogMergePolicy mergePolicy = new LogByteSizeMergePolicy();
		// 设置segment添加文档(Document)时的合并频率
		// 值较小,建立索引的速度就较慢 //值较大,建立索引的速度就较快,>10适合批量建立索引
		mergePolicy.setMergeFactor(indexConf.getMergeFactor());
		// 设置segment最大合并文档(Document)数
		// 值较小有利于追加索引的速度
		// 值较大,适合批量建立索引和更快的搜索
		mergePolicy.setMaxMergeDocs(indexConf.getMaxMergeDocs());
		// if (create){
		// iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		// }else {
		// iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		// }
		final IndexWriter writer = new IndexWriter(FSDirectory.open(path), iwc);
		String similarityName = indexConf.getSimilarityName();
		// Lucene的排序
		if (similarityName != null) {
			try {
				Class<? extends Similarity> similarityClass = Class.forName(similarityName)
						.asSubclass(Similarity.class);
				Constructor<? extends Similarity> ctor = similarityClass.getConstructor(Version.class);
				Similarity similarity = ctor.newInstance();
				iwc.setSimilarity(similarity);
			} catch (Exception e) {
				throw new IOException("Error in creating a similarity object " + similarityName);
			}
		}

		return new RecordWriter<ImmutableBytesWritable, LuceneDocumentWrapper>() {
			AtomicBoolean closed = new AtomicBoolean(false);
			private long docCount = 0;

			public void write(ImmutableBytesWritable key, LuceneDocumentWrapper value) throws IOException {
				Document doc = value.get();
				writer.addDocument(doc);
				docCount++;
				progress.progress();
			}

			public void close(final Reporter reporter) throws IOException {
				Thread prog = new Thread() {
					@Override
					public void run() {
						while (!closed.get()) {
							try {
								reporter.setStatus("closing");
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								continue;
							} catch (Throwable e) {
								return;
							}
						}
					}
				};

				try {
					prog.start();

					// optimize index
					if (indexConf.doOptimize()) {
						if (LOG.isInfoEnabled()) {
							LOG.info("Optimizing index.");
						}
						writer.forceMerge(1);
					}

					// close index
					writer.close();
					if (LOG.isInfoEnabled()) {
						LOG.info("Done indexing " + docCount + " docs.");
					}

					// copy to perm destination in dfs
					fs.completeLocalOutput(perm, temp);
					if (LOG.isInfoEnabled()) {
						LOG.info("Copy done.");
					}
				} finally {
					closed.set(true);
				}
			}
		};
	}
}