--********************************************************************--
--author: jinhua.xu
--create time: 2022.01.19 09:56
--description: hive 行转列/列转行
--********************************************************************--

-- 创建一个测试库
CREATE DATABASE dw;

-- 创建测试数据表
CREATE TABLE IF NOT EXISTS dw.person
(
    name   string,
    gender string,
    age    int,
    hobby  string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
;

-- 数据内容
-- jack,male,20,music
-- marry,female,18,dance
-- carry,male,22,basketball
-- tom,male,24,movie
-- jack,male,20,READ
-- marry,female,18,run
-- tom,male,24,movie

LOAD DATA LOCAL INPATH '/opt/data/person.csv' INTO TABLE dw.person;

-- 行转列：把同一个人的爱好使用,分隔组合,并进行去重
SELECT name, gender, age, concat_ws(",", collect_set(hobby)) AS hobbys
FROM dw.person
GROUP BY name, gender, age
;


-- 行转列：把同一个人的爱好使用,分隔组合,不进行去重
SELECT name, gender, age, concat_ws(",", collect_list(hobby)) AS hobbys
FROM dw.person
GROUP BY name, gender, age
;


CREATE TABLE IF NOT EXISTS dw.user_tag
(
    name string,
    tags string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
;

-- tom,T0001/T0002/T0003
-- jack,T0002/T0001
-- marry,T0003/T0004
-- mac,T0002/T0005

LOAD DATA LOCAL INPATH '/opt/data/tag.csv' INTO TABLE dw.user_tag;

SELECT name, tag
FROM dw.user_tag
         LATERAL VIEW explode(split(tags, "/")) tbl AS tag
;
