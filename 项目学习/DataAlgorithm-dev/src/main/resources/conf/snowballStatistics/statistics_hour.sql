insert into TB_TRACK_STATISTICS_HOUR(ID,FIRST_TIME,FIRST_DEVICE_ID,FIRST_DEVICE_TYPE,FIRST_INFO_ID,LAST_TIME,LAST_DEVICE_ID,LAST_DEVICE_TYPE,LAST_INFO_ID,CNT,DATA_TYPE,PT)
select
	t1.ID,FIRST_TIME, FIRST_DEVICE_ID, FIRST_DEVICE_TYPE,FIRST_INFO_ID,LAST_TIME, LAST_DEVICE_ID, LAST_DEVICE_TYPE, LAST_INFO_ID,CNT,t1.DATA_TYPE, @BATCH_TIME@ as PT
from
    (select
        ID,DATA_TYPE,SHOT_TIME FIRST_TIME, DEVICE_ID FIRST_DEVICE_ID, DEVICE_TYPE FIRST_DEVICE_TYPE, INFO_ID FIRST_INFO_ID
    from
        (select
            ID,INFO_ID,DATA_TYPE,SHOT_TIME, DEVICE_ID, DEVICE_TYPE, rowNumberInAllBlocks() as rank
        from
            (select
                ID,INFO_ID,DATA_TYPE,SHOT_TIME, DEVICE_ID, DEVICE_TYPE
            from TB_TRACK_ALL where PT >= @START_TIME_HOUR@ and PT <= @BATCH_TIME@
            order by ID,DATA_TYPE,SHOT_TIME) a
            ) b limit 1 by ID) t1
    join
    (select
        ID,DATA_TYPE,SHOT_TIME LAST_TIME, DEVICE_ID LAST_DEVICE_ID, DEVICE_TYPE LAST_DEVICE_TYPE, INFO_ID LAST_INFO_ID
    from
        (select
            ID,INFO_ID,DATA_TYPE,SHOT_TIME, DEVICE_ID, DEVICE_TYPE, rowNumberInAllBlocks() as rank
        from
            (select
                ID,INFO_ID,DATA_TYPE,SHOT_TIME, DEVICE_ID, DEVICE_TYPE
            from TB_TRACK_ALL where PT >= @START_TIME_HOUR@ and PT <= @BATCH_TIME@
            order by ID,DATA_TYPE,SHOT_TIME desc) a
            ) b limit 1 by ID) t2
    on t1.ID = t2.ID and t1.DATA_TYPE = t2.DATA_TYPE
   join
   (select
  		ID,DATA_TYPE, count(1) CNT
  	from TB_TRACK_ALL where PT >= @START_TIME_HOUR@ and PT <= @BATCH_TIME@
  	group by ID,DATA_TYPE) t3
  	on t1.ID = t3.ID and t1.DATA_TYPE = t3.DATA_TYPE;
