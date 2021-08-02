-- insert into tb_track_statistics select person_id,count(1) as cnt  from hive_face_detect_rl  where shot_time >=@START_TIME@  and  shot_time <@END_TIME@   group by person_id;

insert into tb_track_all (id,info_id,shot_time,device_id, data_type,device_type, pt)
select person_id as id,info_id,shot_time,device_id, 4 as data_type,device_type,@BATCH_TIME@ as pt
from hive_face_detect_rl where create_time >=@START_TIME@  and  create_time <@END_TIME@ and data_source=8 and person_id is not null and length (person_id)>0;



insert into tb_track_all (id,info_id,shot_time,device_id, data_type,device_type, pt)
select person_id as id,info_id,shot_time,device_id,5 as data_type,device_type,@BATCH_TIME@ as pt
from hive_face_detect_rl where create_time >=@START_TIME@  and  create_time <@END_TIME@ and data_source=17 and person_id is not null and length (person_id)>0;


insert into tb_track_all (id,info_id,shot_time,device_id, data_type,device_type, pt)
select person_id as id,info_id,shot_time,device_id,6 as data_type,device_type,@BATCH_TIME@ as pt
from hive_face_detect_rl where create_time >=@START_TIME@  and  create_time <@END_TIME@ and data_source=13 and person_id is not null and length (person_id)>0;












