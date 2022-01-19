--********************************************************************--
--author: jinhua.xu
--create time: 2022.01.19 10:00
--description: hive window function
--********************************************************************--


CREATE TABLE app.sr_tmp_visits
(
    userId string,
    month  string,
    visits int
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
;

LOAD DATA LOCAL INPATH '/data/disk11/hypersuser/service-report/code-adhoc/adhoc/recent/jira_2542/test/visits.csv' INTO TABLE app.sr_tmp_visits;


-- 可用作面试题
-- 统计每个用户每个月的访问次数以及最大单月访问次数，累计到该月的总访问次数
SELECT userId,
       month,
       visits_count,
       max(visits_count) OVER (PARTITION BY userId ORDER BY month) AS max_count,
       sum(visits_count) OVER (PARTITION BY userId ORDER BY month) AS sum_count
FROM (SELECT userId,
             month,
             sum(visits) AS visits_count
      FROM app.sr_tmp_visits
      GROUP BY userId, month) AS tmp
;


-- 创建用户访问表
CREATE TABLE app.sr_tmp_user
(
    cookieid   string, -- 用户登录的cookie，即用户标识
    createtime string, -- 日期
    pv         int     -- 页面访问量
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
;

-- cookie1,2021-05-10,1
-- cookie1,2021-05-11,5
-- cookie1,2021-05-12,7
-- cookie1,2021-05-13,3
-- cookie1,2021-05-14,2
-- cookie1,2021-05-15,4
-- cookie1,2021-05-16,4
LOAD DATA LOCAL INPATH '/data/disk11/hypersuser/service-report/code-adhoc/adhoc/recent/jira_2542/test/pv.csv' INTO TABLE app.sr_tmp_user;

-- 基础窗口函数的使用sum,max,min,avg
SELECT cookieid,
       createtime,
       pv,
       sum(pv) OVER (PARTITION BY cookieid ORDER BY createtime) AS sum_pv,
       max(pv) OVER (PARTITION BY cookieid ORDER BY createtime) AS max_pv,
       max(pv) OVER (PARTITION BY cookieid)                     AS no_max_pv, -- 注意不加order by排序取的是当前同一分区内的最大值
       min(pv) OVER (PARTITION BY cookieid ORDER BY createtime) AS min_pv,
       avg(pv) OVER (PARTITION BY cookieid ORDER BY createtime) AS avg_pv
FROM app.sr_tmp_user
;

-- 排序窗口函数的使用row_number, rank, dense_rank
SELECT cookieid,
       createtime,
       pv,
       row_number() OVER (PARTITION BY cookieid ORDER BY pv DESC) AS row_num,
       rank() OVER (PARTITION BY cookieid ORDER BY pv DESC)       AS rank,
       dense_rank() OVER (PARTITION BY cookieid ORDER BY pv DESC) AS dense_rank
FROM app.sr_tmp_user
;


-- 复杂窗口函数lag,lead,first_value,last_value
CREATE TABLE app.sr_tmp_user_url
(
    cookieid   string,
    createtime string, --页面访问时间
    url        string  --被访问页面
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
;

LOAD DATA LOCAL INPATH '/data/disk11/hypersuser/service-report/code-adhoc/adhoc/recent/jira_2542/test/url.csv' INTO TABLE app.sr_tmp_user_url;


-- 案例测试
SELECT cookieid,
       createtime,
       url,
       row_number() OVER (PARTITION BY cookieid ORDER BY createtime)                               AS row_num,
       -- 第一个参数为列名，第二个参数为向上第n行，第三个为默认值
       lag(createtime, 1, '1970-01-01 00:00:00') OVER (PARTITION BY cookieid ORDER BY createtime)  AS last_1_time,
       lag(createtime, 2) OVER (PARTITION BY cookieid ORDER BY createtime)                         AS last_2_time,
       -- 第一个参数为列名，第二个参数为向下第n行，第三个为默认值
       lead(createtime, 1, '1970-01-01 00:00:00') OVER (PARTITION BY cookieid ORDER BY createtime) AS lead_1_time,
       lead(createtime, 2) OVER (PARTITION BY cookieid ORDER BY createtime)                        AS lead_2_time,
       -- 取分组排序后，截止到当前行第一个值
       first_value(url) OVER (PARTITION BY cookieid ORDER BY createtime)                           AS first_val,
       -- 取分组排序后，截止到当前行最后一个值
       last_value(url) OVER (PARTITION BY cookieid ORDER BY createtime)                            AS last_val
FROM app.sr_tmp_user_url
;

-- +----------+---------------------+--------+---------+---------------------+---------------------+---------------------+---------------------+-----------+----------+
-- | cookieid | createtime          | url    | row_num | last_1_time         | last_2_time         | lead_1_time         | lead_2_time         | first_val | last_val |
-- +----------+---------------------+--------+---------+---------------------+---------------------+---------------------+---------------------+-----------+----------+
-- | cookie1  | 2021-06-10 10:00:00 | url1   | 1       | 1970-01-01 00:00:00 | NULL                | 2021-06-10 10:00:02 | 2021-06-10 10:03:04 | url1      | url1     |
-- | cookie1  | 2021-06-10 10:00:02 | url2   | 2       | 2021-06-10 10:00:00 | NULL                | 2021-06-10 10:03:04 | 2021-06-10 10:10:00 | url1      | url2     |
-- | cookie1  | 2021-06-10 10:03:04 | 1url3  | 3       | 2021-06-10 10:00:02 | 2021-06-10 10:00:00 | 2021-06-10 10:10:00 | 2021-06-10 10:50:01 | url1      | 1url3    |
-- | cookie1  | 2021-06-10 10:10:00 | url4   | 4       | 2021-06-10 10:03:04 | 2021-06-10 10:00:02 | 2021-06-10 10:50:01 | 2021-06-10 10:50:05 | url1      | url4     |
-- | cookie1  | 2021-06-10 10:50:01 | url5   | 5       | 2021-06-10 10:10:00 | 2021-06-10 10:03:04 | 2021-06-10 10:50:05 | 2021-06-10 11:00:00 | url1      | url5     |
-- | cookie1  | 2021-06-10 10:50:05 | url6   | 6       | 2021-06-10 10:50:01 | 2021-06-10 10:10:00 | 2021-06-10 11:00:00 | NULL                | url1      | url6     |
-- | cookie1  | 2021-06-10 11:00:00 | url7   | 7       | 2021-06-10 10:50:05 | 2021-06-10 10:50:01 | 1970-01-01 00:00:00 | NULL                | url1      | url7     |
-- | cookie2  | 2021-06-10 10:00:00 | url11  | 1       | 1970-01-01 00:00:00 | NULL                | 2021-06-10 10:00:02 | 2021-06-10 10:03:04 | url11     | url11    |
-- | cookie2  | 2021-06-10 10:00:02 | url22  | 2       | 2021-06-10 10:00:00 | NULL                | 2021-06-10 10:03:04 | 2021-06-10 10:10:00 | url11     | url22    |
-- | cookie2  | 2021-06-10 10:03:04 | 1url33 | 3       | 2021-06-10 10:00:02 | 2021-06-10 10:00:00 | 2021-06-10 10:10:00 | 2021-06-10 10:50:01 | url11     | 1url33   |
-- | cookie2  | 2021-06-10 10:10:00 | url44  | 4       | 2021-06-10 10:03:04 | 2021-06-10 10:00:02 | 2021-06-10 10:50:01 | 2021-06-10 10:50:05 | url11     | url44    |
-- | cookie2  | 2021-06-10 10:50:01 | url55  | 5       | 2021-06-10 10:10:00 | 2021-06-10 10:03:04 | 2021-06-10 10:50:05 | 2021-06-10 11:00:00 | url11     | url55    |
-- | cookie2  | 2021-06-10 10:50:05 | url66  | 6       | 2021-06-10 10:50:01 | 2021-06-10 10:10:00 | 2021-06-10 11:00:00 | NULL                | url11     | url66    |
-- | cookie2  | 2021-06-10 11:00:00 | url77  | 7       | 2021-06-10 10:50:05 | 2021-06-10 10:50:01 | 1970-01-01 00:00:00 | NULL                | url11     | url77    |
-- +----------+---------------------+--------+---------+---------------------+---------------------+---------------------+---------------------+-----------+----------+

-- cube, rolling up, grouping sets
CREATE TABLE app.sr_tmp_user_date
(
    month    STRING,
    day      STRING,
    cookieid STRING
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
;

LOAD DATA LOCAL INPATH '/data/disk11/hypersuser/service-report/code-adhoc/adhoc/recent/jira_2542/test/user_date.csv' INTO TABLE app.sr_tmp_user_date;


-- grouping sets是一种将多个group by逻辑写在一个sql语句中的便利写法
SELECT month,
       day,
       count(DISTINCT cookieid) AS uv
FROM app.sr_tmp_user_date
GROUP BY month, day
    GROUPING SETS ( month, day)
;
-- 上面的sql等价于下面的sql
SELECT month,
       NULL                     AS day,
       COUNT(DISTINCT cookieid) AS uv,
       1                        AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month
UNION ALL
SELECT NULL                     AS month,
       day,
       COUNT(DISTINCT cookieid) AS uv,
       2                        AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY day;

-- cube是根据group by的维度的所有组合进行聚合
SELECT month,
       day,
       COUNT(DISTINCT cookieid) AS uv,
       GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month, day
WITH CUBE
ORDER BY GROUPING__ID
;

-- 上面的sql等价于下面的sql写法
SELECT NULL, NULL, COUNT(DISTINCT cookieid) AS uv, 0 AS GROUPING__ID
FROM app.sr_tmp_user_date
UNION ALL
SELECT month, NULL, COUNT(DISTINCT cookieid) AS uv, 1 AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month
UNION ALL
SELECT NULL, day, COUNT(DISTINCT cookieid) AS uv, 2 AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY day
UNION ALL
SELECT month, day, COUNT(DISTINCT cookieid) AS uv, 3 AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month, day;

-- rollup使用：以左侧的维度为主，从该维度进行层级聚合，是cube的子集
SELECT month,
       day,
       COUNT(DISTINCT cookieid) AS uv,
       GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month, day
WITH ROLLUP
ORDER BY GROUPING__ID;

-- 上面的sql等价于下面sql的写法
SELECT NULL, NULL, COUNT(DISTINCT cookieid) AS uv, 0 AS GROUPING__ID
FROM app.sr_tmp_user_date
UNION ALL
SELECT month, NULL, COUNT(DISTINCT cookieid) AS uv, 1 AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month
UNION ALL
SELECT month, day, COUNT(DISTINCT cookieid) AS uv, 3 AS GROUPING__ID
FROM app.sr_tmp_user_date
GROUP BY month, day;


-- 连续登陆多少天的用户 5天
CREATE TABLE app.sr_tmp_login_test
(
    id         string,
    login_date string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
;

-- 1,2020-06-01
-- 1,2020-06-02
-- 1,2020-06-03
-- 1,2020-06-04
-- 1,2020-06-05
-- 5,2020-06-08
-- 5,2020-06-09
-- 5,2020-06-07
-- 5,2020-06-10
-- 5,2020-05-20

LOAD DATA LOCAL INPATH '/data/disk11/hypersuser/service-report/code-adhoc/adhoc/recent/jira_2542/test/login.csv' INTO TABLE app.sr_tmp_login_test;


SELECT DISTINCT id, diff
from
    (SELECT id,
            login_date,
            date_sub(login_date, row_number() OVER (PARTITION BY id ORDER BY login_date)) AS diff
     FROM (SELECT DISTINCT id, login_date
           FROM app.sr_tmp_login_test) AS tmp) AS t1
GROUP BY id, diff
HAVING count(id) >= 4
;
