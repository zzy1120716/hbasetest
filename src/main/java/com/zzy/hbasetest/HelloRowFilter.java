package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: HelloRowFilter
 * @description: 行键过滤器，找出左右 rowkey > row3 的行
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloRowFilter {


    private static byte[] printResult(ResultScanner rs) {
        byte[] lastRowKey = null;
        for (Result r : rs) {
            byte[] rowkey = r.getRow();
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(Bytes.toString(rowkey) + ": name=" + name);
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

            // 行键过滤器
            Filter filter = new RowFilter(CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("row3")));
            scan.setFilter(filter);

            // 执行查询
            ResultScanner rs = table.getScanner(scan);
            printResult(rs);
            rs.close();
        }
    }
}
