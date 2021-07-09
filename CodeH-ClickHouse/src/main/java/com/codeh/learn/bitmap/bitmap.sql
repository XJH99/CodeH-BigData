-- 位图函数的使用

-- 1.无符号整数构建位图对象
select bitmapBuild([1,2,3,4,5]) as res;

-- 2.将位图函数转化为整数数组
select bitmapToArray(bitmapBuild([1,2,3,4,5])) as res;
/*
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
*/

-- 3.bitmapSubsetInRange 将位图指定范围转化为另外一个位图,相当于截取数据，左闭右开
select bitmapToArray(bitmapSubsetInRange(bitmapBuild(
                                                 [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]),
                                         toUInt32(10), toUInt32(15))) as res;

/*
┌─res──────────────┐
│ [10,11,12,13,14] │
└──────────────────┘
 */

-- 4.bitmapSubsetLimit 将位图指定范围转化为另外一个位图,(位图函数，起始点，限制条数)
select bitmapToArray(bitmapSubsetLimit(bitmapBuild(
                                               [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]),
                                       toUInt32(10), toUInt32(15))) as res;

/*
┌─res────────────────────────────────────────────┐
│ [10,11,12,13,14,15,16,17,18,19,20,21,22,23,24] │
└────────────────────────────────────────────────┘
 */


-- 5.bitmapContains 查看位图中是否包含指定元素:存在则返回1，不存在返回0
select bitmapContains(bitmapBuild([1,2,3,4,5]), toUInt32(9)) as res;

-- 6.bitmapHasAny 比较两个位图是否包含相同元素: 如果有相同的返回1，没有则返回0
select bitmapHasAny(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) as res;

-- 7.bitmapHasAll 如果第一个位图包含第二个位图所有元素返回1，反之返回0
select bitmapHasAll(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) as res;

-- 8.bitmapAnd 两个位图进行与操作，返回一个新位图对象 =》 取交
select bitmapToArray(bitmapAnd(bitmapBuild([1,2,3,4,5,6]), bitmapBuild([3,4,5,6,7,8]))) as res;

/*
┌─res───────┐
│ [3,4,5,6] │
└───────────┘
 */

-- 9.bitmapOr 两个位图进行或操作，返回一个新位图对象 =》 取并集，去重数
select bitmapToArray(bitmapOr(bitmapBuild([1,2,3,4,5,6]), bitmapBuild([3,4,5,6,7,8]))) as res;

/*
┌─res───────────────┐
│ [1,2,3,4,5,6,7,8] │
└───────────────────┘
 */

-- 10.bitmapXor 两个位图进行异或操作，返回一个新位图对象 =》 去除两者重复值，其它值合并
select bitmapToArray(bitmapXor(bitmapBuild([1,2,3,4,5,6]), bitmapBuild([3,4,5,6,7,8]))) as res;

/*
┌─res───────┐
│ [1,2,7,8] │
└───────────┘
 */

-- 11.bitmapAndnot 计算两个位图的差异，返回一个新的位图对象 => 第一个位图去除与第二个位图相等值后的数值结果
select bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3,4,5,6]), bitmapBuild([3,4,5,6,7,8]))) as res;

/*
┌─res───┐
│ [1,2] │
└───────┘
 */

-- 12.bitmapCardinality 返回UInt64类型的数值，表示位图对象的数量:下面数量为5
select bitmapCardinality(bitmapBuild([1,2,3,4,6])) as res;

-- 13.bitmapMin 返回返回UInt64类型的数值，表示位图最小值:1
select bitmapMin(bitmapBuild([1,2,3,4,5,6,7,8])) as res;

-- 14.bitmapMax 返回返回UInt64类型的数值，表示位图最大值:8
select bitmapMax(bitmapBuild([1,2,3,4,5,6,7,8])) as res;

-- 15.bitmapAndCardinality 两个位图进行与操作，得到结果的数量:4
SELECT bitmapAndCardinality(bitmapBuild([1,2,3,4,5,6]), bitmapBuild([3,4,5,6,7,8])) AS res;

-- 16.bitmapOrCardinality 两个位图进行或操作，得到结果的数量:8
SELECT bitmapOrCardinality(bitmapBuild([1,2,3,4,5,6]), bitmapBuild([3,4,5,6,7,8])) AS res;

-- 17.bitmapXorCardinality 两个位图进行异或操作，得到结果的数量:4
SELECT bitmapXorCardinality(bitmapBuild([1,2,3,4,5,6,7]), bitmapBuild([3,4,5,6,7,8,9])) AS res;

-- 18.bitmapAndnotCardinality 位图和非标准性, 计算两个位图的差异，返回结果位图的基数:2
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3,4,5,6,7]), bitmapBuild([3,4,5,6,7,8,9])) AS res;

-- 19.聚合类函数的使用 groupBitmapAnd，groupBitmapOr，groupBitmapXor

-- 数据准备
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z      AggregateFunction(groupBitmap, UInt32)
)
    ENGINE = MergeTree
        ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2
VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2
VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2
VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2
VALUES ('tag4', bitmapBuild(cast([2,4,6,8,10,12,12,10] as Array(UInt32))));

-- 查询测试
SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
/* 原理是所有的bitmap对象取交，得到的数量，
┌─groupBitmapAnd(z)─┐
│                 3 │
└───────────────────┘
*/
SELECT bitmapToArray(groupBitmapAndState(z)) FROM bitmap_column_expr_test2 WHERE tag_id = 'tag4';
/*  对bitmap对象里面的数据进行去重操作，后转为array
┌─bitmapToArray(groupBitmapAndState(z))─┐
│ [2,4,6,8,10,12]                       │
└───────────────────────────────────────┘
 */
SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
/*  所有匹配的bitmap对象取并集，去重的数量
┌─groupBitmapOr(z)─┐
│               15 │
└──────────────────┘
 */
SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
/*  去除两者重复值，其它值合并(每两个bitmap对象进行一次操作)，得到最后的数量
┌─groupBitmapOr(z)─┐
│               10 │
└──────────────────┘
 */


---------------------------------------------- 案例测试 ------------------------------------

-- 用户标签数据表
create table if not exists hypers.label_uid(
    label_name String,
    label_value UInt32,
    uv AggregateFunction(groupBitmap, UInt8)
)engine = AggregatingMergeTree()
partition by label_name
order by (label_name,label_value)
SETTINGS index_granularity = 32;  -- 由于标签的行数不会太多，将索引粒度调整适量大小（默认8192）
;


-- 用户信息表
create table if not exists hypers.base_user(
    id UInt8,
    name String,
    country String,
    city String,
    tag UInt32,
    create_time DateTime
)engine = MergeTree
partition by toYYYYMM(create_time)
order by id
;

-- 插入用户样例数据
insert into hypers.base_user values (1, 'zhangsan', '中国', '上海', 10001, now()),
                                    (2, 'lisi', '中国', '北京', 10002, now()),
                                    (3, 'wangwu', '中国', '南昌', 10002, now()),
                                    (4, 'pig', '中国', '深圳', 10003, now()),
                                    (5, 'dog', '中国', '四川', 10004, now()),
                                    (6, 'cat', '中国', '重庆', 10005, now());


-- 将用户数据导入标签表中

insert into hypers.label_uid
select 'id', tag, groupBitmapState(id)
from hypers.base_user
group by tag
;


-- 查询标签数据
select label_name, label_value, bitmapToArray(groupBitmapMergeState(uv))
from hypers.label_uid
group by label_name, label_value;

/*
┌─label_name─┬─label_value─┬─bitmapToArray(groupBitmapMergeState(uv))─┐
│ id         │       10004 │ [5]                                      │
│ id         │       10001 │ [1]                                      │
│ id         │       10002 │ [2,3]                                    │
│ id         │       10005 │ [6]                                      │
│ id         │       10003 │ [4]                                      │
└────────────┴─────────────┴──────────────────────────────────────────┘
 */
