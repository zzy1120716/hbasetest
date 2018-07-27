package com.zzy.mymr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @ClassName: SumMoneyJob
 * @description: 统计mymoney表中info:income的总和
 * @author: 赵正阳
 * @date: 2018-07-26 16:58
 * @version: V1.0
 **/
public class SumMoneyJob {

    /**
     * 用来获取每一条记录的 info:income 的 Mapper 类
     * @author zzy
     */
    static class MyTableMapper extends TableMapper<Text, IntWritable> {

        private Text text = new Text("allIncomes");

        /**
         * TableMapper 类需要实现的 map 方法
         *
         * @param row
         * @param result
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException {
            // 当 Scan 遍历每一条记录的时候，这个方法都会被调用一次
            // 获取该行记录的 info:income
            IntWritable income = new IntWritable(Bytes.toInt(result.getValue("info".getBytes(), "income".getBytes())));
            // 把拿到的值全部写入 allIncomes 这个键中
            context.write(text, income);
        }
    }




    /**
     * 用来把 Mapper 获取到的记录进行统计的 Reduce 方法
     * @author zzy
     */
    static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        /**
         * TableReducer 需要实现的 reduce 方法
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 每个键都会进行一次 reduce 操作
            // 在这个方法中把 allIncomes 这个键之前存储的所有 info:income 进行叠加取得总和
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            // 叠加完后，剩下的事情就是把总和存起来
            Put put = new Put(Bytes.toBytes("total"));
            put.addColumn("info".getBytes(), "totalIncome".getBytes(), Bytes.toBytes(i));
            // 把这个 Put 写到 Context 里面，随后这个 Put 会被自动执行
            context.write(null, put);
        }

    }




    public static void main(String[] args) throws Exception {

        // 由于 MapReduce 任务是在服务端执行的，所以不需要像之前那样设置 config 文件的位置
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config, "SumMyMoney");
        job.setJarByClass(SumMoneyJob.class);   // 包含我们写的 MyTableMapper 和 MyTableReducer 的类

        // 创建给这个 MapReduce 任务使用的扫描器对象
        Scan scan = new Scan();
        scan.setCaching(500);   // 不要使用默认值1，这样性能太低了，所以改成500
        scan.setCacheBlocks(false);   // 在 MapReduce 任务中你也不想拿到缓存吧？所以我们设置成 false

        // 使用HBase的工具类来帮我们为Job对象设置 Mapper 和 Reducer
        TableMapReduceUtil.initTableMapperJob(
                "mymoney",      // 输入表
                scan,
                MyTableMapper.class,    // 写上我们的 Mapper 类
                Text.class,             // Mapper 输出的 Key 类型
                IntWritable.class,      // Mapper 输出的 Value 类型
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                "mymoney",      // 输出表
                MyTableReducer.class,   // 写上我们的 Reducer 类
                job
        );

        // 最后设置Job的其他属性
        // 至少 1 个 Reduce 任务
        job.setNumReduceTasks(1);

        // 任务执行中会提示进度
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("任务出错！");
        }
    }


}
