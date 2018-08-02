package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;

/**
 * @ClassName: HelloFuzzyRowFilter
 * @description: 模糊行键过滤器
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloFuzzyRowFilter {


    private static byte[] printResult(ResultScanner rs) {
        byte[] lastRowKey = null;
        for (Result r : rs) {
            byte[] rowkey = r.getRow();
            String actionName = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("actionName")));
            String userId = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("userId")));
            System.out.println("row=" + Bytes.toString(rowkey) + ", actionName=" + actionName + ", userId=" + userId);
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

            FuzzyRowFilter filter = new FuzzyRowFilter(Arrays.asList(
                    new Pair<byte[], byte[]>(
                            Bytes.toBytesBinary("2016_??_??_4567"),
                            new byte[] {0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0}
                    )));

            scan.setFilter(filter);

            // 执行查询
            ResultScanner rs = table.getScanner(scan);
            printResult(rs);
            rs.close();
        }
    }
}
