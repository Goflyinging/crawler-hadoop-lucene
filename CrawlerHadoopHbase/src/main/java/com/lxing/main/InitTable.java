package com.lxing.main;

import com.lxing.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Description:
 * @author: 路星星
 * @version: 1.0
 * @date: 0:39 2017/6/1
 */
public class InitTable {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        conf.addResource("app-config.xml");
        try {
            HbaseUtil.initTable(conf);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
