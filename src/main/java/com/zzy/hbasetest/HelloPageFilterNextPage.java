package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: HelloPageFilterNextPage
 * @description: 分页过滤器实现翻页
 * @author: 赵正阳
 * @date: 2018-07-27 11:25
 * @version: V1.0
 **/
public class HelloPageFilterNextPage {

    private static byte[] printResult(ResultScanner rs) {
        byte[] lastRowKey = null;
        for (Result r : rs) {
            byte[] rowkey = r.getRow();
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            int age = Bytes.toInt(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("age")));
            System.out.println(Bytes.toString(rowkey) + ": name=" + name + " age=" + age);
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

            Filter filter = new PageFilter(2L);
            scan.setFilter(filter);

            // 第1页
            ResultScanner rs = table.getScanner(scan);

            // 自己实现的打印结果方法
            byte[] lastRowKey = printResult(rs);
            rs.close();

            System.out.println("现在打印第2页");

            // 第2页
            // 为 lastRowkey 拼接上一个零字节
            // 防止第二次的 Scan 结果集把第一次的最后一条记录包含进去
            byte[] startRowkey = Bytes.add(lastRowKey, new byte[1]);
            scan.setStartRow(startRowkey);
            ResultScanner rs2 = table.getScanner(scan);

            // 自己实现的打印结果方法
            printResult(rs2);
            rs2.close();

        }
    }
}
