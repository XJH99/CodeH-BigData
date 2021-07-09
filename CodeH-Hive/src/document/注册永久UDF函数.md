### 注册永久UDF函数

1.将本地打好的jar包上传到生产环境的本地目录

2.将生产环境本地的jar包上传到hdfs上

```shell
hdfs dfs -put CodeH-Hive-1.0-SNAPSHOT.jar /data/jar/
```

3.注册永久函数

```sql
# 注意这里hdfs的路径一定要为全路径
create function md5 AS 'com.hypers.udf.Md5' USING JAR 'hdfs://hadoop102:9000/data/jar/CodeH-Hive-1.0-SNAPSHOT.jar'; 
```

4.测试

```sql
select md5("14790789902");

# 结果：567f0f833cda525338716c16571fdde9
```

### 注册临时UDF函数

只需要在第三步加上temporary 关键字即可，注册后只能在当前会话中使用，退出hive环境就会失效

```sql
# 注意这里hdfs的路径一定要为全路径
create temporary function md5 AS 'com.hypers.udf.Md5' USING JAR 'hdfs://hadoop102:9000/data/jar/CodeH-Hive-1.0-SNAPSHOT.jar'; 
```

