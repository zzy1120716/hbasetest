package com.zzy.mymr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: SumMoneyJobResult
 * @description: 查看MapReduce结果
 * @author: 赵正阳
 * @date: 2018-07-26 17:59
 * @version: V1.0
 **/
public class SumMoneyJobResult {

    public static void main(String[] args) throws Exception {

        // 获取配置文件
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        try (Connection connection = ConnectionFactory.createConnection(config)){

            Table table = connection.getTable(TableName.valueOf("mymoney"));
            Get get = new Get(Bytes.toBytes("total"));
            Result result = table.get(get);
            System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("totalIncome"))));
        }
    }
}
