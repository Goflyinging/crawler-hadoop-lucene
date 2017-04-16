package com.lxing.util;

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
 * Created by lxing on 2017/4/14.
 */
public class HbaseUtil {
    public static Logger logger = LoggerFactory.getLogger(HbaseUtil.class);
    
    /***
     * 清除Hbase表中的中间表 CacheSaveTable和cacheArticleTable
     * 
     * @param conf
     *            Hbase参数信息
     * @return ture 清除成功 false 清除失败
     * @throws IOException
     */
    public static boolean clearCache(Configuration conf) throws IOException {
        String saveTable = conf.get("html.table.name");
        String cacheSaveTable = "cache" + saveTable;
        String articleTable = conf.get("article.table.name");
        String cacheArticleTable = "cache" + articleTable;
        if (saveTable == null || articleTable == null) {
            logger.error("获取表名失败");
            return false;
        }
        Connection conn = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        admin.disableTable(cacheSaveTable);
        admin.disableTable(cacheArticleTable);
        admin.truncateTable(TableName.valueOf(cacheSaveTable), false);
        admin.truncateTable(TableName.valueOf(cacheArticleTable), false);
        return true;
    }
    
    /***
     * 建立程序运行过程中需要的表
     * 
     * @param conf
     *            Hbase参数信息
     * @throws Exception
     */
    public static void initTable(Configuration conf) throws Exception {
        ArrayList<String> list = new ArrayList<>();
        // url集合表
        list.add(conf.get("url.table.name"));
        // 网页信息保存表
        list.add(conf.get("html.table.name"));
        list.add("cache" + conf.get("html.table.name"));
        // 文章信息保存表
        list.add(conf.get("article.table.name"));
        list.add("cache" + conf.get("article.table.name"));
        Connection conn = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        for (String tableName : list) {
            if (!admin.tableExists(tableName)) {
                logger.info(tableName + "not exits ,start create it...");
                HTableDescriptor desc =
                                      new HTableDescriptor(TableName.valueOf(tableName));
                desc.addFamily(new HColumnDescriptor("info".getBytes()));
                admin.createTable(desc);
                logger.info("create success");
            }
            else {
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
