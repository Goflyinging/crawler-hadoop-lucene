package com.lxing.inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class InjectDriver {
    
    public static Logger logger = LoggerFactory.getLogger(InjectDriver.class);
    
    /***
     * 从本地文件插入url到Hbase.url表中 本地文件路径可以从app-config.xml文件中设置（input.path）
     * 
     * @param conf
     *            hbase配置信息
     * @return 1表示成功 0表示失败
     */
    public int injectFromLocal(Configuration conf) {
        String tableName = conf.get("url.table.name");
        if (tableName == null || "" == tableName.trim())
            logger.info("read url.table.name fail");
        String inputPath = conf.get("input.path");
        if (inputPath == null || "" == tableName.trim())
            logger.info("read input.path fail");
        Connection conn = null;// 连接hbase
        BufferedReader br = null;// 读取本地文件url集合
        try {
            br = new BufferedReader(new FileReader(inputPath));
            String s;
            HashSet<String> set = new HashSet<String>();
            while ((s = br.readLine()) != null) {
                logger.info("read URL：" + s);
                set.add(s);
            }
            conn = ConnectionFactory.createConnection(conf);
            logger.info("begin inset data：");
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            List<Put> list = new ArrayList<Put>();
            for (String url : set) {
                Put put = new Put(url.getBytes());
                put.addColumn("info".getBytes(),
                              "status".getBytes(),
                              "0".getBytes());
                logger.info("url：" + url);
                list.add(put);
            }
            table.put(list);
        }
        catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (br != null) {
                try {
                    br.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return 1;
    }
    
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");
        conf.addResource("app-config.xml");
        InjectDriver inject = new InjectDriver();
        int i = inject.injectFromLocal(conf);
        if (i == 0) {
            System.out.print("error！！！");
        }
        
    }
    
}
