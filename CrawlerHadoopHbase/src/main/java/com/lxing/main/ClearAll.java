package com.lxing.main;

import com.lxing.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * @Description:
 * @author: 路星星
 * @version: 1.0
 * @date: 0:33 2017/6/1
 */
public class ClearAll {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("app-config.xml");
        conf.addResource("hbase-site.xml");
        HbaseUtil.clearAll(conf);
    }

}
