package com.zzy.hbasetest;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName: Test
 * @description: 测试
 * @author: 赵正阳
 * @date: 2018-07-26 18:31
 * @version: V1.0
 **/
public class Test {

    public static void main(String[] args) {
        System.out.println(Bytes.toInt(Bytes.toBytes("~\\x82C@")));
    }
}
