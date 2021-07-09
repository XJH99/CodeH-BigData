--********************************************************************--
--author: jinhua.xu
--create time: 2021.03.31 11:04
--description: 用户标签性能测试
--********************************************************************--


-- xuan_session_id_label_info_v2 原始用户标签表
/*
┌─uid─┬─session_id───────────────────────────┬─label─────────────────────────────────────────────┬─values─────────────────────────────────┬─────date─┐
│   0 │ 00000812-20bb-45ca-9d07-8967fa66e352 │ [10000,10100,10200,10300,10400,10502,11111,22222] │ ([10500,10500,10500],[5.29,5.29,5.29]) │ 20210316 │
└─────┴──────────────────────────────────────┴───────────────────────────────────────────────────┴────────────────────────────────────────┴──────────┘
 */

-- xuan_user_mapping_list_v1_all 用户映射表
/*
┌─user_id─┬─session_id───────────────────────────┐
│       6 │ 0eaed015-3b76-4c22-92d9-a3ce88594f6d │
└─────────┴──────────────────────────────────────┘
 */

-- xuan_label_bituser_v2 标签-用户id集合（bitmap存储）
SELECT label_id, bitmapToArray(groupBitmapMergeState(bituser))
FROM cluster.xuan_label_bituser_v2
GROUP BY label_id
LIMIT 1;


------------------------------------------------------- 通过用户来找标签 --------------------------------------------------

-- 单独的用户查询标签
SELECT session_id, label from cluster.xuan_session_id_label_info_v2 where session_id = '00006df0-5b88-40c3-b512-67accbad4e72';
/*
localhost:9000, queries 213, QPS: 213.201, RPS: 4366355.618, MiB/s: 187.465, result RPS: 213.201, result MiB/s: 0.017.

0.000%		0.004 sec.
10.000%		0.004 sec.
20.000%		0.004 sec.
30.000%		0.004 sec.
40.000%		0.005 sec.
50.000%		0.005 sec.
60.000%		0.005 sec.
70.000%		0.005 sec.
80.000%		0.005 sec.
90.000%		0.005 sec.
95.000%		0.005 sec.
99.000%		0.005 sec.
99.900%		0.006 sec.
99.990%		0.006 sec.
*/

-- 两个不同的用户取出相同标签
SELECT
    arrayIntersect(a.label, b.label)
from
(SELECT label from cluster.xuan_session_id_label_info_v2 where session_id = '00006df0-5b88-40c3-b512-67accbad4e72') as a
,(SELECT label from cluster.xuan_session_id_label_info_v2 where session_id = 'b90022e8-ce7d-4422-8474-d384b05df8e9') as b
;




------------------------------------------------------- 通过标签找用户 ---------------------------------------------------

-- 通过标签找到用户
SELECT session_id, label from cluster.xuan_session_id_label_info_v2 where has(label,10502);

/*
localhost:9000, queries 5, QPS: 0.429, RPS: 14653808.078, MiB/s: 1187.872, result RPS: 4883747.117, result MiB/s: 395.888.

0.000%		2.105 sec.
10.000%		2.105 sec.
20.000%		2.242 sec.
30.000%		2.242 sec.
40.000%		2.386 sec.
50.000%		2.386 sec.
60.000%		2.386 sec.
70.000%		2.431 sec.
80.000%		2.431 sec.
90.000%		2.479 sec.
95.000%		2.479 sec.
99.000%		2.479 sec.
99.900%		2.479 sec.
99.990%		2.479 sec.
 */

-- 统计具有某个标签的用户有多少
SELECT count(*) from cluster.xuan_session_id_label_info_v2 where has(label,10502);
/*
localhost:9000, queries 15, QPS: 3.174, RPS: 108292987.220, MiB/s: 4131.050, result RPS: 3.174, result MiB/s: 0.000.

0.000%		0.298 sec.
10.000%		0.299 sec.
20.000%		0.300 sec.
30.000%		0.301 sec.
40.000%		0.314 sec.
50.000%		0.323 sec.
60.000%		0.323 sec.
70.000%		0.326 sec.
80.000%		0.326 sec.
90.000%		0.330 sec.
95.000%		0.330 sec.
99.000%		0.330 sec.
99.900%		0.330 sec.
99.990%		0.330 sec.
 */





