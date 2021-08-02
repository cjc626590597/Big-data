-- insert into tb_track_statistics select person_id,count(1) as cnt  from hive_face_detect_rl  where shot_time >=@START_TIME@  and  shot_time <@END_TIME@   group by person_id;

with tmp1 as
(select person_id as id,info_id,shot_time,device_id, 4 as data_type,device_type,@BATCH_TIME@ as pt
from hive_face_detect_rl where create_time >=@START_TIME@  and  create_time <@END_TIME@ and data_source=8 and person_id is not null and length (person_id)>0)
insert into tb_track_statistics_min(id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,pt,cnt)
select id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type, last_info_id,pt, count(1) from(
select id,data_type,
first_value(shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_time,
first_value(device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_id,
first_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_type,
first_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_info_id,
last_value (shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_time,
last_value (device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_id,
last_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_type,
last_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_info_id,
pt
from tmp1
) t group by id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,pt;


with tmp2 as
(select person_id as id,info_id,shot_time,device_id,5 as data_type,device_type,@BATCH_TIME@ as pt
from hive_face_detect_rl where create_time >=@START_TIME@  and  create_time <@END_TIME@ and data_source=17 and person_id is not null and length (person_id)>0)
insert into tb_track_statistics_min(id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,pt,cnt)
select id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type, last_info_id,pt, count(1) from(
select id,data_type,
first_value(shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_time,
first_value(device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_id,
first_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_type,
first_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_info_id,
last_value (shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_time,
last_value (device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_id,
last_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_type,
last_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_info_id,
pt
from tmp2
) t group by id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,pt;


with tmp3 as
(select person_id as id,info_id,shot_time,device_id,6 as data_type,device_type,@BATCH_TIME@ as pt
from hive_face_detect_rl where create_time >=@START_TIME@  and  create_time <@END_TIME@ and data_source=13 and person_id is not null and length (person_id)>0)
insert into tb_track_statistics_min(id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,pt,cnt)
select id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type, last_info_id,pt, count(1) from(
select id,data_type,
first_value(shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_time,
first_value(device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_id,
first_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_type,
first_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_info_id,
last_value(shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_time,
last_value(device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_id,
last_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_type,
last_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_info_id,
pt
from tmp3
) t group by id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,pt;












