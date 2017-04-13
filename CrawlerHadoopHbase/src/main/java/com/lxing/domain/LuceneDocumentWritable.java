package com.lxing.domain;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;

import java.io.DataInput;
import java.io.DataOutput;

public class LuceneDocumentWritable implements Writable {
    protected Document doc;

    public LuceneDocumentWritable(Document doc) {
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