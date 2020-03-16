--表A结构为：
CREATE TABLE user_visit_log(
     ds STRING COMMENT '日期 分区字段',
     user_id BIGINT COMMENT 'Uin',
     visit_time BIGINT COMMENT '访问时间戳 单位 s',
     scene BIGINT COMMENT '进入应用号场景 ',
     sessionid STRING COMMENT '单次访问会话 id',
     appid STRING COMMENT 'appid'
)
--用sql获取当天访问人数TOP10的小程序当天最后一个会话访问的用户、场景和会话id

--TOP10小程序的最后访问记录

set hive.groupby.skewindata = true --针对数据倾斜问题
解法一：
SELECT appid, user_id, scene, sessionid
  FROM (
        SELECT appid, user_id, scene, sessionid
               ,ROW_NUMBER()OVER(PARTITION BY appid ORDER BY user_cnt DESC) AS u_rank
          FROM (
                SELECT appid
                       ,COUNT(DISTINCT user_id             ) AS user_cnt
                       ,MAX(IF(v_rank = 1, user_id  , NULL)) AS user_id
                       ,MAX(IF(v_rank = 1, scene    , NULL)) AS scene
                       ,MAX(IF(v_rank = 1, sessionid, NULL)) AS sessionid
                  FROM (
                        SELECT appid, user_id, scene, sessionid
                               ,ROW_NUMBER()OVER(PARTITION BY appid ORDER BY visit_time DESC) AS v_rank
                         FROM user_visit_log
                        WHERE ds = DATE_SUB(CURRENT_DATE,0)
                        ) t1
                 GROUP BY appid
               ) t2
       ) t3
 WHERE u_rank <= 10;

解法二：
WITH tmp_appid_cnt AS (
SELECT appid
  FROM (
        SELECT  appid
               ,ROW_NUMBER()OVER(PARTITION BY appid ORDER BY user_cnt DESC) AS u_rank
          FROM (
                SELECT appid, COUNT(user_id) AS user_cnt
                  FROM (
				        SELECT appid, user_id
				          FROM user_visit_log
						WHERE ds = DATE_SUB(CURRENT_DATE,0)
						GROUP BY appid, user_id
	                )t1
                GROUP BY appid
               ) t2
       ) t3
 WHERE u_rank <= 10
)

SELECT b.appid, b.user_id, b.scene, b.sessionid
  FROM tmp_appid_cnt a
  INNER JOIN (
        SELECT appid, user_id, scene, sessionid
          FROM (
                SELECT appid, user_id, scene, sessionid
                       ,ROW_NUMBER()OVER(PARTITION BY appid ORDER BY visit_time DESC) AS v_rank
                  FROM user_visit_log
                 WHERE ds = DATE_SUB(CURRENT_DATE,0)
               ) t
         WHERE v_rank = 1
       ) b
    ON a.appid = b.appid;

--对于数据倾斜问题,可通过观察数据针对不同分布式计算框架调整参数再进一步解决 
