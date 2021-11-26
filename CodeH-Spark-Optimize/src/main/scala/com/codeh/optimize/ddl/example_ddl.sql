--********************************************************************--
--author: jinhua.xu
--create time: 2021.11.26 19:04
--description: 样例sql定义语句
--********************************************************************--

CREATE TABLE `sparktuning.sale_course`
(
    `chapterid`      bigint,
    `chaptername`    string,
    `courseid`       bigint,
    `coursemanager`  string,
    `coursename`     string,
    `edusubjectid`   bigint,
    `edusubjectname` string,
    `majorid`        bigint,
    `majorname`      string,
    `money`          string,
    `pointlistid`    bigint,
    `status`         string,
    `teacherid`      bigint,
    `teachername`    string
)
    PARTITIONED BY (
        `dt` string,
        `dn` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'path' = 'hdfs://codeh01:9820/user/hive/warehouse/sparktuning.db/sale_course')
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        'hdfs://codeh01:9820/user/hive/warehouse/sparktuning.db/sale_course'
    TBLPROPERTIES (
        'spark.sql.create.version' = '3.0.0',
        'spark.sql.partitionProvider' = 'catalog',
        'spark.sql.sources.provider' = 'parquet',
        'spark.sql.sources.schema.numPartCols' = '2',
        'spark.sql.sources.schema.numParts' = '1',
        'spark.sql.sources.schema.part.0' =
                '{"type":"struct","fields":[{"name":"chapterid","type":"long","nullable":true,"metadata":{}},{"name":"chaptername","type":"string","nullable":true,"metadata":{}},{"name":"courseid","type":"long","nullable":true,"metadata":{}},{"name":"coursemanager","type":"string","nullable":true,"metadata":{}},{"name":"coursename","type":"string","nullable":true,"metadata":{}},{"name":"edusubjectid","type":"long","nullable":true,"metadata":{}},{"name":"edusubjectname","type":"string","nullable":true,"metadata":{}},{"name":"majorid","type":"long","nullable":true,"metadata":{}},{"name":"majorname","type":"string","nullable":true,"metadata":{}},{"name":"money","type":"string","nullable":true,"metadata":{}},{"name":"pointlistid","type":"long","nullable":true,"metadata":{}},{"name":"status","type":"string","nullable":true,"metadata":{}},{"name":"teacherid","type":"long","nullable":true,"metadata":{}},{"name":"teachername","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"dn","type":"string","nullable":true,"metadata":{}}]}',
        'spark.sql.sources.schema.partCol.0' = 'dt',
        'spark.sql.sources.schema.partCol.1' = 'dn',
        'transient_lastDdlTime' = '1637917828')
;

CREATE TABLE `sparktuning.course_shopping_cart`
(
    `courseid`   bigint,
    `coursename` string,
    `createtime` string,
    `discount`   string,
    `orderid`    string,
    `sellmoney`  string
)
    PARTITIONED BY (
        `dt` string,
        `dn` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'path' = 'hdfs://codeh01:9820/user/hive/warehouse/sparktuning.db/course_shopping_cart')
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        'hdfs://codeh01:9820/user/hive/warehouse/sparktuning.db/course_shopping_cart'
    TBLPROPERTIES (
        'spark.sql.create.version' = '3.0.0',
        'spark.sql.partitionProvider' = 'catalog',
        'spark.sql.sources.provider' = 'parquet',
        'spark.sql.sources.schema.numPartCols' = '2',
        'spark.sql.sources.schema.numParts' = '1',
        'spark.sql.sources.schema.part.0' =
                '{"type":"struct","fields":[{"name":"courseid","type":"long","nullable":true,"metadata":{}},{"name":"coursename","type":"string","nullable":true,"metadata":{}},{"name":"createtime","type":"string","nullable":true,"metadata":{}},{"name":"discount","type":"string","nullable":true,"metadata":{}},{"name":"orderid","type":"string","nullable":true,"metadata":{}},{"name":"sellmoney","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"dn","type":"string","nullable":true,"metadata":{}}]}',
        'spark.sql.sources.schema.partCol.0' = 'dt',
        'spark.sql.sources.schema.partCol.1' = 'dn',
        'transient_lastDdlTime' = '1637918444')
;

CREATE TABLE `sparktuning.course_pay`
(
    `createtime` string,
    `discount`   string,
    `orderid`    string,
    `paymoney`   string
)
    PARTITIONED BY (
        `dt` string,
        `dn` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'path' = 'hdfs://codeh01:9820/user/hive/warehouse/sparktuning.db/course_pay')
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        'hdfs://codeh01:9820/user/hive/warehouse/sparktuning.db/course_pay'
    TBLPROPERTIES (
        'spark.sql.create.version' = '3.0.0',
        'spark.sql.partitionProvider' = 'catalog',
        'spark.sql.sources.provider' = 'parquet',
        'spark.sql.sources.schema.numPartCols' = '2',
        'spark.sql.sources.schema.numParts' = '1',
        'spark.sql.sources.schema.part.0' =
                '{"type":"struct","fields":[{"name":"createtime","type":"string","nullable":true,"metadata":{}},{"name":"discount","type":"string","nullable":true,"metadata":{}},{"name":"orderid","type":"string","nullable":true,"metadata":{}},{"name":"paymoney","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"dn","type":"string","nullable":true,"metadata":{}}]}',
        'spark.sql.sources.schema.partCol.0' = 'dt',
        'spark.sql.sources.schema.partCol.1' = 'dn',
        'transient_lastDdlTime' = '1637917110')
;