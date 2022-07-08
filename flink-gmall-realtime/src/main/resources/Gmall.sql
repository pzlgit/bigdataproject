CREATE TABLE topic_db
(
    `database` string,
    `table`    string,
    `type`     string,
    `data`     map<string,string>,
    `old`      map<string,string>,
    `ts`       string,
    `proc_time` AS proctime()
) WITH (
      'connector' = 'kafka',
      'topic' = 'topic_db',
      'properties.bootstrap.servers' = 'hadoop102:9092',
      'properties.group.id' = 'dwd_trade_cart_add',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'json'
      )
;

select data['id']                                                                       id,
       data['user_id']                                                                  user_id,
       data['sku_id']                                                                   sku_id,
       data['source_id']                                                                source_id,
       data['source_type']                                                              source_type,
       if(`type` = 'insert', data['sku_num'],
          cast(cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,
       ts,
       proc_time
from topic_db
where `table` = 'cart_info'
  and (`type` = 'insert' or (type = 'update' and `old`['sku_num'] is not null and
                             cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)));


CREATE TABLE base_dic
(
    dic_code     string,
    dic_name     string,
    parent_code  string,
    create_time  timestamp,
    operate_time timestamp,
    PRIMARY KEY (dic_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://hadoop102:3306/gmall',
    'table-name' = 'base_dic',
    'username' = 'root',
    'password' = '123456',
    'lookup.cache.max-rows' = '500',
    'lookup.cache.ttl' = '1 hour',
    'driver' = 'com.mysql.cj.jdbc.driver'
);

select id,
       user_id,
       sku_id,
       source_id,
       source_type,
       dic_name source_type_name,
       sku_num,
       ts
from cart_add c
         join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b
on c.source_type = b.dic_code

CREATE TABLE dwd_trade_cart_add
(
    id               string,
    user_id          string,
    sku_id           string,
    source_id        string,
    source_type      string,
    source_type_name string,
    sku_num          string,
    ts               string,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_trade_cart_add',
  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092',
  'key.format' = 'json',
  'value.format' = 'json',
);
