package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import java.io.IOException;

/**
 * @ClassName: HelloHBase
 * @description: hbase客户端api
 * @author: 赵正阳
 * @date: 2018-07-23 20:42
 * @version: V1.0
 **/
public class HelloHBase {

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
            TableName tableName = TableName.valueOf("mytable");

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

    /**
     * 添加新的列族，并更新已有列族
     *
     * @param config
     * @throws IOException
     */
    public static void modifySchema(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()){

            TableName tableName = TableName.valueOf("mytable");

            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist. ");
                System.exit(-1);
            }

            // 往 mytable 里面添加 newcf 列族
            HColumnDescriptor newColumn = new HColumnDescriptor("newcf");
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // 获取表定义
            HTableDescriptor table = admin.getTableDescriptor(tableName);

            // 更新 mycf 这个列族
            HColumnDescriptor mycf = new HColumnDescriptor("mycf");
            mycf.setCompactionCompressionType(Algorithm.GZ);
            mycf.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(mycf);
            admin.modifyTable(tableName, table);
        }
    }

    /**
     * 删除表操作
     *
     * @param config
     * @throws IOException
     */
    public static void deleteSchema(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin()){

            TableName tableName = TableName.valueOf("mytable");

            // 停用(Disable) mytable
            admin.disableTable(tableName);

            // 删除掉 mycf 列族
            admin.deleteColumn(tableName, "mycf".getBytes("UTF-8"));

            // 删除 mytable 表(一定要记得先停用表)
            admin.deleteTable(tableName);
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

        // 改表
        modifySchema(config);

        // 删表
        deleteSchema(config);

    }
}
