package com.lxing.index;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;

public class IndexTableReduce extends MapReduceBase
		implements Reducer<ImmutableBytesWritable, Result, ImmutableBytesWritable, LuceneDocumentWrapper> {
	private static final Log LOG = LogFactory.getLog(IndexTableReduce.class);
	private IndexConfiguration indexConf;

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		indexConf = new IndexConfiguration();
		String content = job.get("hbase.index.conf");
		if (content != null) {
			indexConf.addFromXML(content);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Index conf: " + indexConf);
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	public void reduce(ImmutableBytesWritable key, Iterator<Result> values,
			OutputCollector<ImmutableBytesWritable, LuceneDocumentWrapper> output, Reporter reporter)
			throws IOException {
		Document doc = null;
		while (values.hasNext()) {
			Result r = values.next();
			// 将hbase 表中的行键存储在document对象中
			if (doc == null) {
				doc = new Document();
				// index and store row key, row key already UTF-8 encoded
				Field keyField = new StringField(indexConf.getRowkeyName(),
						Bytes.toString(key.get(), key.getOffset(), key.getLength()), Field.Store.YES);
				doc.add(keyField);
			}
			// 遍历hbase表中除行键以外的列r.listCells()
			for (Cell cell : r.listCells()) {
				// 复制列名和列值
				String column = Bytes.toString(CellUtil.cloneFamily(cell));
				byte[] columnValue = CellUtil.cloneValue(cell);
				// 从配置信息里获取是否存储这些field默认只分析不存储
				//IndexOptions indexOptions = indexConf.isIndex(column) ? IndexOptions.DOCS : IndexOptions.NONE;
				// 索引列
				// 打分参数设置
				FieldType fieldType = new FieldType();
				fieldType.setOmitNorms(indexConf.isOmitNorms(column));
				fieldType.setTokenized(indexConf.isTokenize(column));
				fieldType.setStored(indexConf.isStore(column));
				fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

				Field field = new Field(column, Bytes.toString(columnValue), fieldType);
				field.setBoost(indexConf.getBoost(column));
				doc.add(field);
			}
		}
		output.collect(key, new LuceneDocumentWrapper(doc));
	}
}