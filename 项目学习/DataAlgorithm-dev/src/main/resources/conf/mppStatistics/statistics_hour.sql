




--统计每五分钟的增量。todo 时间要改;
ALTER TABLE tb_track_statistics_hour  ADD PARTITION P@PARTITION_TIME_HOUR@ VALUES LESS THAN (@PARTITION_TIME_HOUR@);

-- insert into tb_track_statistics_hour(id,first_time,first_device_id,last_time,last_device_id,cnt,data_type,pt)
-- select  id,first_time,first_device_id,last_time,last_device_id, count(1), data_type ,  @BATCH_TIME@ as pt  from(
-- select   id, first_value(shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_time,
-- first_value(device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_id,
-- last_value (shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_time,
-- last_value (device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_id,data_type
--  from tb_track_all where shot_time >=@START_TIME_HOUR@  and  shot_time <@BATCH_TIME@
--  ) t group by id,data_type,first_time,first_device_id,last_time,last_device_id;


insert into tb_track_statistics_hour(id,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,cnt,data_type,pt)
select  id,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id, count(1), data_type ,  @BATCH_TIME@ as pt  from(
select   id, first_value(shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_time,
first_value(device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_id,
first_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_type,
first_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_info_id,
last_value (shot_time)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_time,
last_value (device_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_id,
last_value(device_type)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_type,
last_value(info_id)over(partition by id,data_type order by shot_time  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_info_id,
data_type
 from tb_track_all where pt >=@START_TIME_HOUR@  and  pt <=@BATCH_TIME@
 ) t group by id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id;






