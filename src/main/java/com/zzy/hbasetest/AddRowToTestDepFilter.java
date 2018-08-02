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
 * @ClassName: AddRowToTestDepFilter
 * @description: 增加行
 * @author: 赵正阳
 * @date: 2018-07-23 20:42
 * @version: V1.0
 **/
public class AddRowToTestDepFilter {

    public static void main(String[] args) throws Exception {

        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("testDepFilter"));

            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), 1L, Bytes.toBytes("jack"));
            put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("updatedTime"), 1L, Bytes.toBytes("nothing"));
            table.put(put);

            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), 1L, Bytes.toBytes("ted"));
            put2.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("updatedTime"), 1L, Bytes.toBytes("nothing"));
            table.put(put2);

            Put put3 = new Put(Bytes.toBytes("row3"));
            put3.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), 1L, Bytes.toBytes("billy"));
            put3.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("updatedTime"), 1L, Bytes.toBytes("nothing"));
            table.put(put3);

            Put put4 = new Put(Bytes.toBytes("row4"));
            put4.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), 1L, Bytes.toBytes("sara"));
            put4.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("updatedTime"), 1L, Bytes.toBytes("nothing"));
            table.put(put4);
        }
    }
}
