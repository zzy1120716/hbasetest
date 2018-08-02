package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

/**
 * @ClassName: HelloRandomRowFilter
 * @description: 随机行过滤器
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloRandomRowFilter {


    private static void printResult(ResultScanner rs) {
        int i = 1;
        for (Iterator<Result> it = rs.iterator(); it.hasNext(); i++) {
            byte[] rowkey = it.next().getRow();
            System.out.println("第 " + i + " 行(rowkey=" + Bytes.toString(rowkey) + ")");
        }
    }


    public static void main(String[] args) throws Exception {
        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("mytable"));
            Scan scan = new Scan(Bytes.toBytes("row1"));

            // 初始化随机行过滤器
            Filter filter = new RandomRowFilter(new Float("0.5"));
            scan.setFilter(filter);

            // 执行查询
            ResultScanner rs = table.getScanner(scan);
            printResult(rs);
            rs.close();
        }
    }
}
