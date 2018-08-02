package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: AddRowToMyTable
 * @description: 增加行
 * @author: 赵正阳
 * @date: 2018-07-23 20:42
 * @version: V1.0
 **/
public class AddRowToMyTable {

    public static void main(String[] args) throws Exception {

        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("mytable"));
            List<Put> putList = new ArrayList<>();
            putList.add(new Put(Bytes.toBytes("2016_09_11_4567")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("actionName"), Bytes.toBytes("xx")));
            putList.add(new Put(Bytes.toBytes("2016_09_11_4567")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("userId"), Bytes.toBytes("abc")));
            putList.add(new Put(Bytes.toBytes("2016_12_12_4567")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("actionName"), Bytes.toBytes("xx")));
            putList.add(new Put(Bytes.toBytes("2016_12_12_4567")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("userId"), Bytes.toBytes("abc")));
            table.put(putList);
        }
    }
}
