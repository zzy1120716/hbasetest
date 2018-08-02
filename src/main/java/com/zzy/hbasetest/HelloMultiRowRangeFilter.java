package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: HelloMultiRowRangeFilter
 * @description: 多行范围过滤器
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloMultiRowRangeFilter {


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
            Scan scan = new Scan();

            // 多行范围过滤器
            // 构建从 row1 到 row2 的 RowRange
            RowRange rowRange1to2 = new RowRange("row1", true, "row2", true);

            // 构建从 row5 到 row7 的 RowRange
            RowRange rowRange5to7 = new RowRange("row5", true, "row7", true);

            // 构造 RowRange 的 List
            List<RowRange> rowRanges = new ArrayList<RowRange>();

            // 添加这两个 RowRange 到 List 中
            rowRanges.add(rowRange1to2);
            rowRanges.add(rowRange5to7);

            // 初始化 MultiRowRangeFilter
            Filter multiRangeFilter = new MultiRowRangeFilter(rowRanges);

            // 为 Scan 设置 Filter
            scan.setFilter(multiRangeFilter);

            // 执行查询
            ResultScanner rs = table.getScanner(scan);
            printResult(rs);
            rs.close();
        }
    }
}
