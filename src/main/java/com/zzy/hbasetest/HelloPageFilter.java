package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: HelloPageFilter
 * @description: 分页过滤器
 * @author: 赵正阳
 * @date: 2018-07-27 11:25
 * @version: V1.0
 **/
public class HelloPageFilter {

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

            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                String rowKey = Bytes.toString(r.getRow());
                String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
                int age = Bytes.toInt(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("age")));
                System.out.println(rowKey + ": name=" + name + " age=" + age);
            }
            rs.close();
        }
    }
}
