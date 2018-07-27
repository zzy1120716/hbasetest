package com.zzy.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: HelloFilterList
 * @description: 使用过滤器列表
 * @author: 赵正阳
 * @date: 2018-07-27 10:25
 * @version: V1.0
 **/
public class HelloFilterList {

    public static void main(String[] args) throws Exception {
        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("mytable"));
            Scan scan = new Scan();

            // 创建过滤器列表
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            // 只有列族为 mycf 的记录才放入结果集
            Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("mycf")));
            filterList.addFilter(familyFilter);

            // 只有列为 teacher 的记录才放入结果集
            Filter colFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("teacher")));
            filterList.addFilter(colFilter);

            // 只有值包含 Wang 的记录才放入结果集
            Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("Wang"));
            filterList.addFilter(valueFilter);

            scan.setFilter(filterList);

            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                String teacher = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("teacher")));
                System.out.println(teacher);
            }
            rs.close();
        }
    }
}
