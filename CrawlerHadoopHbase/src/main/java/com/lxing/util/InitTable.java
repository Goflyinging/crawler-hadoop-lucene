package com.lxing.util;

import com.lxing.inject.InjectDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by lxing on 2017/4/10.
 */
public class InitTable {
    public static Logger logger = LoggerFactory.getLogger(InjectDriver.class);

    public static void initTable(Configuration conf) throws Exception {
        ArrayList<String> list = new ArrayList<>();
        //url集合表
        list.add(conf.get("url.table.name"));
        //网页信息保存表
        list.add(conf.get("html.table.name"));
        list.add("cache"+conf.get("html.table.name"));
        //文章信息保存表
        list.add(conf.get("article.table.name"));
        list.add("cache"+conf.get("article.table.name"));
        Connection conn = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        for (String tableName : list) {
            if (!admin.tableExists(tableName)) {
                logger.info(tableName + "not exits ,start create it...");
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
                desc.addFamily(new HColumnDescriptor("info".getBytes()));
                admin.createTable(desc);
                logger.info("create success");
            } else {
                logger.info(tableName + " exits ,please confirm it...");
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        conf.addResource("app-config.xml");
        try {
            initTable(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
