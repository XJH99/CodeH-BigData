--********************************************************************--
--author: jinhua.xu
--create time: 2021.03.31 16:14
--description: 创建模拟数据
--********************************************************************--

-- 创建原始三方标签数据表
CREATE TABLE IF NOT EXISTS cluster.original_segment_3rd_label_info
(
    id         Int32,
    session_id String,
    label      Array(UInt32),
    date       DateTime
) ENGINE = MergeTree
      PARTITION BY toYYYYMMDD(date)
      ORDER BY session_id
;


-- 通过随机函数制造数据，并导入到原始标签表中
-- range(random + random + 100000, 105000, random +1) 第三个参数表示第一个参数累加的基数
INSERT INTO cluster.original_segment_3rd_label_info
SELECT id,
       session_id,
       label,
       date
FROM (SELECT toInt32(number + 1)                                               AS id,     -- 自增的编号
             toString(generateUUIDv4())                                        AS session_id,
             rand32()                                                          AS rand32, -- 返回一个UInt32类型的随机数字
             modulo(rand32, 10)                                                AS pid,    -- 对10取余操作，得到随机的个位数
             rand64()                                                          AS rand64, -- 返回一个UInt64类型的随机数字
             modulo(rand32, 10) + modulo(rand32, 5) + 1                        AS random, -- 得到一个随机数
             arraySlice(range(random + random + 100000, 105000, random + 1), 1,
                        modulo(random, 100))                                   AS label,  -- 得到随机的标签数组
             modulo(rand32, 20)                                                AS rand1,
             modulo(rand64, 20)                                                AS rand2,
             range(rand1 + 100000, rand2 + rand1 + 100000 + 1)                 AS arr,
             range(rand1 + 1, rand2 + rand1 + 1)                               AS values,
             (arr, arrayMap(value -> toFloat32(divide(rand1, value)), values)) AS tuple,
             now()                                                             AS date
      FROM numbers(100000000)) AS tmp
;


----------------------------------------------- 将表结构数据转为用户bitmap格式 ---------------------------------------------

-- 创建用户标签数据表(用户为bitmap格式)
CREATE TABLE IF NOT EXISTS cluster.user_label_bitmap
(
    label_value UInt32,
    userId_bit  AggregateFunction(groupBitmap, UInt32) -- bitmap存储userId
) ENGINE = AggregatingMergeTree
      PARTITION BY label_value
      ORDER BY label_value
      SETTINGS index_granularity = 32;
-- 由于标签的行数不会太多，将索引粒度调整适量大小（默认8192）


-- 将用户原始数据导入到bitmap格式的表中
INSERT INTO cluster.user_label_bitmap
SELECT label_value, groupBitmapState(id)
FROM (SELECT toUInt32(id) AS id, arrayJoin(label AS tmp) AS label_value
      FROM cluster.original_segment_3rd_label_info
         ) AS t
GROUP BY label_value
;


-- 查询数据
SELECT label_value, bitmapToArray(groupBitmapMergeState(userId_bit))
from cluster.user_label_bitmap
WHERE label_value = 100033
GROUP BY label_value
;
