-- 创建一张与hbase关联的hive表
CREATE TABLE hive_hbase_emp_table
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:co
            mm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");

-- 中间表，用于加载文件中的数据
CREATE TABLE emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--1001,tom,mamger,1,20210513,30000.0,999.0,001
--1002,jack,laji,2,20210513,2000.0,888.0,100

LOAD DATA LOCAL INPATH '/opt/data/emp.txt' OVERWRITE INTO TABLE emp;

-- 将中间表的数据加载到原始hive表中
INSERT INTO TABLE hive_hbase_emp_table
SELECT *
FROM emp;

-- 查看hive数据
SELECT *
FROM hive_hbase_emp_table;

-- 查看hbase数据
--scan 'hbase_emp_table'

------------------------------------------------------------------------------------------------------------------------

-- 创建一张外部表来关联上一步hbase中创建的表
CREATE EXTERNAL TABLE relevance_hbase_emp
(
    empno    int,
    ename    string,
    job      string,
    mgr      int,
    hiredate string,
    sal      double,
    comm     double,
    deptno   int
)
    STORED BY
        'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES ("hbase.columns.mapping" =
            ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:co
            mm,info:deptno")
    TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");

-- 可以使用这张外部hive表来分析hbase中的数据