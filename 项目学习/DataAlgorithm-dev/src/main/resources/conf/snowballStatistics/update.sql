
--- update.sql
insert into TB_TRACK_STATISTICS(ID,FIRST_TIME,FIRST_DEVICE_ID,FIRST_DEVICE_TYPE,FIRST_INFO_ID,LAST_TIME,LAST_DEVICE_ID,LAST_DEVICE_TYPE,LAST_INFO_ID,ALL_CNT,CNT,DATA_TYPE,PT)
select
    case when s.ID is null then t.ID else s.ID end as ID,
    (case when s.FIRST_TIME is null and t.FIRST_TIME is not null  then  t.FIRST_TIME
      when t.FIRST_TIME is null and s.FIRST_TIME is not null  then  s.FIRST_TIME
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME>=t.FIRST_TIME then t.FIRST_TIME
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME<t.FIRST_TIME then s.FIRST_TIME END) as FIRST_TIME,
    (case when s.FIRST_TIME is null and t.FIRST_TIME is not null  then  t.FIRST_DEVICE_ID
      when t.FIRST_TIME is null and s.FIRST_TIME is not null  then  s.FIRST_DEVICE_ID
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME>=t.FIRST_TIME then t.FIRST_DEVICE_ID
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME<t.FIRST_TIME then s.FIRST_DEVICE_ID END)
      as FIRST_DEVICE_ID,
    (case when s.FIRST_TIME is null and t.FIRST_TIME is not null  then  t.FIRST_DEVICE_TYPE
      when t.FIRST_TIME is null and s.FIRST_TIME is not null  then  s.FIRST_DEVICE_TYPE
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME>=t.FIRST_TIME then t.FIRST_DEVICE_TYPE
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME<t.FIRST_TIME then s.FIRST_DEVICE_TYPE END)
      as FIRST_DEVICE_TYPE,
    (case when s.FIRST_TIME is null and t.FIRST_TIME is not null  then  t.FIRST_INFO_ID
      when t.FIRST_TIME is null and s.FIRST_TIME is not null  then  s.FIRST_INFO_ID
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME>=t.FIRST_TIME then t.FIRST_INFO_ID
      when s.FIRST_TIME is not null and t.FIRST_TIME is not null  and s.FIRST_TIME<t.FIRST_TIME then s.FIRST_INFO_ID END)
      as FIRST_INFO_ID,
    (case when s.LAST_TIME is null and t.LAST_TIME is not null  then  t.LAST_TIME
      when t.LAST_TIME is null and s.LAST_TIME is not null  then  s.LAST_TIME
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME>=t.LAST_TIME then s.LAST_TIME
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME<t.LAST_TIME then t.LAST_TIME END)
      as LAST_TIME,
    (case when s.LAST_TIME is null and t.LAST_TIME is not null  then  t.LAST_DEVICE_ID
      when t.LAST_TIME is null and s.LAST_TIME is not null  then  s.LAST_DEVICE_ID
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME>=t.LAST_TIME then s.LAST_DEVICE_ID
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME<t.LAST_TIME then t.LAST_DEVICE_ID END)
      as LAST_DEVICE_ID,
    (case when s.LAST_TIME is null and t.LAST_TIME is not null  then  t.LAST_DEVICE_TYPE
      when t.LAST_TIME is null and s.LAST_TIME is not null  then  s.LAST_DEVICE_TYPE
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME>=t.LAST_TIME then s.LAST_DEVICE_TYPE
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME<t.LAST_TIME then t.LAST_DEVICE_TYPE END)
      as LAST_DEVICE_TYPE,
    (case when s.LAST_TIME is null and t.LAST_TIME is not null  then  t.LAST_INFO_ID
      when t.LAST_TIME is null and s.LAST_TIME is not null  then  s.LAST_INFO_ID
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME>=t.LAST_TIME then s.LAST_INFO_ID
      when s.LAST_TIME is not null and t.LAST_TIME is not null  and s.LAST_TIME<t.LAST_TIME then t.LAST_INFO_ID END)
      as LAST_INFO_ID,
    (case when t.ALL_CNT is null then s.CNT
      else t.ALL_CNT end) as ALL_CNT,
    (case
        when s.CNT is null then t.CNT
        else s.CNT
    end) as CNT,
    (case
        when t.DATA_TYPE is null then s.DATA_TYPE
        else t.DATA_TYPE
    end) as DATA_TYPE,
    @BATCH_TIME@ as PT
from
    (select  ID,FIRST_TIME,FIRST_DEVICE_ID,FIRST_DEVICE_TYPE,FIRST_INFO_ID,LAST_TIME,LAST_DEVICE_ID,LAST_DEVICE_TYPE,LAST_INFO_ID,ALL_CNT,CNT,DATA_TYPE,PT from TB_TRACK_STATISTICS where PT=@BATCH_TIME@ )t
    full outer join
    (select
		t1.ID ID,FIRST_TIME, FIRST_DEVICE_ID, FIRST_DEVICE_TYPE,FIRST_INFO_ID,LAST_TIME, LAST_DEVICE_ID, LAST_DEVICE_TYPE, LAST_INFO_ID, CNT,t1.DATA_TYPE DATA_TYPE, @BATCH_TIME@ as PT
	from
    (select
        ID,DATA_TYPE,FIRST_TIME, FIRST_DEVICE_ID, FIRST_DEVICE_TYPE,FIRST_INFO_ID
    from
        (select
            ID,DATA_TYPE,FIRST_TIME, FIRST_DEVICE_ID, FIRST_DEVICE_TYPE, FIRST_INFO_ID, rowNumberInAllBlocks() as rank
        from
            (select
                ID,DATA_TYPE,FIRST_TIME, FIRST_DEVICE_ID, FIRST_DEVICE_TYPE, FIRST_INFO_ID
            from TB_TRACK_STATISTICS_HOUR
            order by ID,DATA_TYPE,PT) a
            ) b limit 1 by ID) t1
    join
    (select
        ID,DATA_TYPE,LAST_TIME, LAST_DEVICE_ID, LAST_DEVICE_TYPE,LAST_INFO_ID
    from
        (select
            ID,DATA_TYPE,LAST_TIME, LAST_DEVICE_ID, LAST_DEVICE_TYPE,LAST_INFO_ID, rowNumberInAllBlocks() as rank
        from
            (select
                ID,DATA_TYPE,LAST_TIME, LAST_DEVICE_ID, LAST_DEVICE_TYPE,LAST_INFO_ID
            from TB_TRACK_STATISTICS_HOUR
            order by ID,DATA_TYPE,PT desc) a
            ) b limit 1 by ID) t2
    on t1.ID = t2.ID and t1.DATA_TYPE = t2.DATA_TYPE
   join
   (select
  		ID,DATA_TYPE, sum(CNT) CNT
  	from TB_TRACK_STATISTICS_HOUR
  	group by ID,DATA_TYPE) t3
  	on t1.ID = t3.ID and t1.DATA_TYPE = t3.DATA_TYPE) s
  	on s.ID = t.ID  and s.DATA_TYPE=t.DATA_TYPE;
