
drop table if   exists tb_track_statistics_tmp;
create table   tb_track_statistics_tmp as
select
 case when s.id is null then t.id else s.id end as id,
 case when s.first_time is null and t.first_time is not null  then  t.first_time
      when t.first_time is null and s.first_time is not null  then  s.first_time
      when s.first_time is not null and t.first_time is not null  and s.first_time>=t.first_time then t.first_time
      when s.first_time is not null and t.first_time is not null  and s.first_time<t.first_time then s.first_time END
      as first_time,
 case when s.first_time is null and t.first_time is not null  then  t.first_device_id
      when t.first_time is null and s.first_time is not null  then  s.first_device_id
      when s.first_time is not null and t.first_time is not null  and s.first_time>=t.first_time then t.first_device_id
      when s.first_time is not null and t.first_time is not null  and s.first_time<t.first_time then s.first_device_id END
      as first_device_id,
  case when s.first_time is null and t.first_time is not null  then  t.first_device_type
      when t.first_time is null and s.first_time is not null  then  s.first_device_type
      when s.first_time is not null and t.first_time is not null  and s.first_time>=t.first_time then t.first_device_type
      when s.first_time is not null and t.first_time is not null  and s.first_time<t.first_time then s.first_device_type END
      as first_device_type,
  case when s.first_time is null and t.first_time is not null  then  t.first_info_id
      when t.first_time is null and s.first_time is not null  then  s.first_info_id
      when s.first_time is not null and t.first_time is not null  and s.first_time>=t.first_time then t.first_info_id
      when s.first_time is not null and t.first_time is not null  and s.first_time<t.first_time then s.first_info_id END
      as first_info_id,
  case when s.last_time is null and t.last_time is not null  then  t.last_time
      when t.last_time is null and s.last_time is not null  then  s.last_time
      when s.last_time is not null and t.last_time is not null  and s.last_time>=t.last_time then s.last_time
      when s.last_time is not null and t.last_time is not null  and s.last_time<t.last_time then t.last_time END
      as last_time,
  case when s.last_time is null and t.last_time is not null  then  t.last_device_id
      when t.last_time is null and s.last_time is not null  then  s.last_device_id
      when s.last_time is not null and t.last_time is not null  and s.last_time>=t.last_time then s.last_device_id
      when s.last_time is not null and t.last_time is not null  and s.last_time<t.last_time then t.last_device_id END
      as last_device_id,
  case when s.last_time is null and t.last_time is not null  then  t.last_device_type
      when t.last_time is null and s.last_time is not null  then  s.last_device_type
      when s.last_time is not null and t.last_time is not null  and s.last_time>=t.last_time then s.last_device_type
      when s.last_time is not null and t.last_time is not null  and s.last_time<t.last_time then t.last_device_type END
      as last_device_type,
  case when s.last_time is null and t.last_time is not null  then  t.last_info_id
      when t.last_time is null and s.last_time is not null  then  s.last_info_id
      when s.last_time is not null and t.last_time is not null  and s.last_time>=t.last_time then s.last_info_id
      when s.last_time is not null and t.last_time is not null  and s.last_time<t.last_time then t.last_info_id END
      as last_info_id,
  (case when t.all_cnt is null then s.cnt
	  else t.all_cnt end) ,
	(case
		when s.cnt is null then t.cnt
		else s.cnt
	end) ,
		(case
		when t.data_type is null then s.data_type
		else t.data_type
	end) as data_type
	,
	@BATCH_TIME@ as pt
from
  (select  id,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,all_cnt,cnt,data_type,pt from tb_track_statistics where pt=@BATCH_TIME@ )t
  full outer  join
 (
select  id,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id, sum(cnt) as cnt, data_type ,  @BATCH_TIME@ as pt  from(
select   id, first_value(first_time)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_time,
first_value(first_device_id)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_id,
first_value(first_device_type)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_device_type,
first_value(first_info_id)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_info_id,
last_value (last_time)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_time,
last_value (last_device_id)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_id,
last_value (last_device_type)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device_type,
last_value (last_info_id)over(partition by id,data_type order by pt  asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_info_id,
data_type,cnt
 from tb_track_statistics_hour
 ) t group by id,data_type,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id
 )s   on s.id = t.id  and s.data_type=t.data_type;



delete  tb_track_statistics where pt= @BATCH_TIME@ and  (select count(1) from tb_track_statistics_tmp )>0 ;

insert into tb_track_statistics(id,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,all_cnt,cnt,data_type,pt)
select id,first_time,first_device_id,first_device_type,first_info_id,last_time,last_device_id,last_device_type,last_info_id,all_cnt,cnt,data_type,pt from tb_track_statistics_tmp
where  (select count(1) from tb_track_statistics_tmp )>0 ;

drop table if   exists tb_track_statistics_tmp;