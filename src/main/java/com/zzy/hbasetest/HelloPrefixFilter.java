package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: HelloPrefixFilter
 * @description: 前缀过滤器
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloPrefixFilter {


    private static byte[] printResult(ResultScanner rs) {
        byte[] lastRowKey = null;
        for (Result r : rs) {
            byte[] rowkey = r.getRow();
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            String city = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("city")));
            String active = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("active")));
            System.out.println(Bytes.toString(rowkey) + ": name=" + name + " city=" + city + " active=" + active);
            lastRowKey = rowkey;
        }
        return lastRowKey;
    }


    public static void main(String[] args) throws Exception {
        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("mytable"));
            Scan scan = new Scan();

            PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("row"));
            scan.setFilter(prefixFilter);

            // 执行查询
            ResultScanner rs = table.getScanner(scan);
            printResult(rs);
            rs.close();
        }
    }
}
