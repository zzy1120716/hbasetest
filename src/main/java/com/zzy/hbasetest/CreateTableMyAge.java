package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: CreateTableMyAge
 * @description: 创建存储数值的表
 * @author: 赵正阳
 * @date: 2018-07-23 20:42
 * @version: V1.0
 **/
public class CreateTableMyAge {

    /**
     * 检查一下mytable表是否存在，如果存在就删掉旧表再重新建立
     *
     * @param admin
     * @param table
     * @throws IOException
     */
    private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            // 如果存在就删掉mytable
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    /**
     * 建立mytable表
     *
     * @param config
     * @throws IOException
     */
    public static void createSchemaTables(Configuration config) throws IOException {
        // 创建连接
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            // 定义表名
            TableName tableName = TableName.valueOf("myage");

            // 定义表
            HTableDescriptor table = new HTableDescriptor(tableName);

            // 定义列族
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            table.addFamily(new HColumnDescriptor(mycf).setCompressionType(Algorithm.NONE));

            System.out.println("Creating table. ");
            // 新建表
            createOrOverwrite(admin, table);
            System.out.println("Done. ");
        }
    }

    public static void main(String[] args) throws Exception {

        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        // 建表
        createSchemaTables(config);

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("myage"));
            List<Put> putList = new ArrayList<>();
            putList.add(new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(9)));
            putList.add(new Put(Bytes.toBytes("row2")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(22)));
            putList.add(new Put(Bytes.toBytes("row3")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(11)));
            putList.add(new Put(Bytes.toBytes("row4")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(16)));
            putList.add(new Put(Bytes.toBytes("row5")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(5)));
            putList.add(new Put(Bytes.toBytes("row6")).addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("age"), Bytes.toBytes(66)));
            table.put(putList);
        }
    }
}
