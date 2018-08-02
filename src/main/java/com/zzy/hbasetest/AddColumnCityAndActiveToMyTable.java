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
 * @ClassName: AddColumnCityAndActiveToMyTable
 * @description: 增加city和active两列
 * @author: 赵正阳
 * @date: 2018-07-23 20:42
 * @version: V1.0
 **/
public class AddColumnCityAndActiveToMyTable {

    public static void main(String[] args) throws Exception {

        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("mytable"));
            List<Put> putList = new ArrayList<>();
            putList.add(new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("beijing")));
            putList.add(new Put(Bytes.toBytes("row2")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("xiamen")));
            putList.add(new Put(Bytes.toBytes("row3")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("shanghai")));
            putList.add(new Put(Bytes.toBytes("row4")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("beijing")));
            putList.add(new Put(Bytes.toBytes("row5")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("shanghai")));
            putList.add(new Put(Bytes.toBytes("row6")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("xiamen")));
            putList.add(new Put(Bytes.toBytes("row7")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("city"), Bytes.toBytes("beijing")));

            putList.add(new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            putList.add(new Put(Bytes.toBytes("row2")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            putList.add(new Put(Bytes.toBytes("row3")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            putList.add(new Put(Bytes.toBytes("row4")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            putList.add(new Put(Bytes.toBytes("row5")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            putList.add(new Put(Bytes.toBytes("row6")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            putList.add(new Put(Bytes.toBytes("row7")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("active"), Bytes.toBytes("1")));
            table.put(putList);
        }
    }
}
