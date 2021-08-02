-- insert into tb_track_statistics select person_id,count(1) as cnt  from hive_face_detect_rl  where shot_time >=@START_TIME@  and  shot_time <@END_TIME@   group by person_id;

insert into tb_track_all (id,info_id,shot_time,device_id, data_type,device_type, pt)
select vehicle_id as id,info_id,pass_time as shot_time,device_id,1 as data_type,device_type,@BATCH_TIME@ as pt
from ARCHIVE_VEHICLE_DETECT_RL where create_time >=@START_TIME@  and  create_time <@END_TIME@  and vehicle_id is not null and length (vehicle_id)>0;












