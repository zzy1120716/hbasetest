# 《HBase不睡觉书》
## HBase高级特性
```
create 'mytable','mycf'
put 'mytable','row1','mycf:name','billyWangpaul'
put 'mytable','row2','mycf:name','sara'
put 'mytable','row3','mycf:name','chris'
put 'mytable','row4','mycf:name','helen'
put 'mytable','row5','mycf:name','andyWang'
put 'mytable','row6','mycf:name','kateWang'
```

```
put 'mytable','row2','mycf:teacher','lilyWang'
```

```
put 'mytable','row7','mycf:name','Wang'
```

### 依赖列过滤器
#### 准备数据
```
> create 'testDepFilter','mycf'
```
利用Java API插入记录（AddRowToTestDepFilter）

## HBase + MapReduce
### 1. 快速入门
```
export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_CLASSPATH=`hbase classpath`
hadoop jar /opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib/hbase/hbase-server-1.2.0-cdh5.15.0.jar rowcounter mytable
```

### 2. 慢速入门
#### 准备数据
```
create 'mymoney','info'
put 'mymoney','01','info:income','6000'
put 'mymoney','01','info:expense','5000'
put 'mymoney','02','info:income','6600'
put 'mymoney','02','info:expense','5300'
put 'mymoney','03','info:income','4000'
put 'mymoney','03','info:expense','5200'
put 'mymoney','04','info:income','5310'
put 'mymoney','04','info:expense','5320'
put 'mymoney','05','info:income','4500'
put 'mymoney','05','info:expense','4800'
put 'mymoney','06','info:income','5500'
put 'mymoney','06','info:expense','4500'
put 'mymoney','07','info:income','5600'
put 'mymoney','07','info:expense','5200'
put 'mymoney','08','info:income','4900'
put 'mymoney','08','info:expense','5100'
put 'mymoney','09','info:income','5600'
put 'mymoney','09','info:expense','5200'
put 'mymoney','10','info:income','6900'
put 'mymoney','10','info:expense','5900'
put 'mymoney','11','info:income','5800'
put 'mymoney','11','info:expense','6100'
put 'mymoney','12','info:income','5700'
put 'mymoney','12','info:expense','6000'
```

#### 编写程序
#### 打包，要用与服务器相同版本的jdk，因此使用1.7
```
mvn clean
mvn package -DskipTests
```
将编译好的jar包传到服务器

#### 运行
```
export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_CLASSPATH=`hbase classpath`
hadoop jar hbasetest-1.0-SNAPSHOT.jar com.zzy.mymr.SumMoneyJob
```

#### 查看结果
```
hbase shell
> scan 'mymoney'
```
表中多了一行：
    
     total                     column=info:totalIncome, timestamp=1532598839903, value=~\x82C@

HBase的shell中无法直接看出数字类型的值，所以最好使用API来获取info:totalIncome的值  
也可以使用HBase提供的方法在命令行中查看：
```
org.apache.hadoop.hbase.util.Bytes.toInt("~\x82C@".to_java_bytes)
```