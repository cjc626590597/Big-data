select min (t) from (
select  min (create_time) t  from ARCHIVE_VEHICLE_DETECT_RL  where length (create_time)>=14 union all
 select  min(create_time)t  from ARCHIVE_CODE_DETECT_RL where length (create_time)>=14 union all
select  min(create_time) t from hive_face_detect_rl where length (create_time)>=14 ) s
