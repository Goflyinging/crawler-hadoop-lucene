package com.lxing.index;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;

public class LuceneDocumentWrapper implements Writable {
  protected Document doc;

  public LuceneDocumentWrapper(Document doc) {
    this.doc = doc;
  }

  public Document get() {
    return doc;
  }

  public void readFields(DataInput in) {
	  
  }

  public void write(DataOutput out) {
  }
}