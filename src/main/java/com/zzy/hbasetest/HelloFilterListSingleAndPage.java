package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: HelloFilterListSingleAndPage
 * @description: 包含自定义过滤器
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloFilterListSingleAndPage {


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

            List<Filter> filters = new ArrayList<Filter>();

            // 设置条件 name like '%Wang%'
            Filter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("mycf"), Bytes.toBytes("name"), CompareOp.EQUAL, new SubstringComparator("Wang"));
            filters.add(nameFilter);

            // 设置分页为每页 2 条数据
            Filter pageFilter = new PageFilter(2L);
            filters.add(pageFilter);

            // 交换两个过滤器的顺序，结果如何？
            /*Filter pageFilter = new PageFilter(2L);
            filters.add(pageFilter);
            Filter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("mycf"), Bytes.toBytes("name"), CompareOp.EQUAL, new SubstringComparator("Wang"));
            filters.add(nameFilter);*/

            // 使用 filters 列表创建 FilterList
            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);

            // 执行查询
            ResultScanner rs = table.getScanner(scan);
            printResult(rs);
            rs.close();
        }
    }
}
