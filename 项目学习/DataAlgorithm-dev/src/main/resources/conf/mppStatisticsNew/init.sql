CREATE table  IF NOT EXISTS  tb_track_statistics
(
     id          varchar(50),
    first_time  BIGINT,
    first_device_id    varchar(50),
    first_device_type int4,
    first_info_id     varchar(64),
    last_time        BIGINT,
    last_device_id    varchar(50),
    last_device_type int4,
    last_info_id     varchar(64),
    all_cnt  integer,
    cnt integer,
    data_type TINYINT,
    pt BIGINT
)
WITH (ORIENTATION = ROW )
DISTRIBUTE BY HASH (id,data_type)
PARTITION BY RANGE(pt)
(
       PARTITION P20200101000500 VALUES LESS THAN(20200101000500)
);

CREATE table  IF NOT EXISTS  tb_track_statistics_min
(
    id          varchar(50),
    first_time  BIGINT,
    first_device_id    varchar(50),
    first_device_type int4,
    first_info_id     varchar(64),
    last_time        BIGINT,
    last_device_id    varchar(50),
    last_device_type int4,
    last_info_id     varchar(64),
    cnt integer,
    data_type TINYINT,
    pt BIGINT
)
WITH (ORIENTATION = ROW )
DISTRIBUTE BY HASH (id,data_type)
PARTITION BY RANGE(pt)
(
       PARTITION P20200101000500 VALUES LESS THAN(20200101000500)
);

CREATE table  IF NOT EXISTS  tb_track_statistics_hour
(
    id          varchar(50),
    first_time  BIGINT,
    first_device_id    varchar(50),
    first_device_type int4,
    first_info_id     varchar(64),
    last_time        BIGINT,
    last_device_id    varchar(50),
    last_device_type int4,
    last_info_id     varchar(64),
    cnt integer,
    data_type TINYINT,
    device_type int4,
    pt BIGINT
)
WITH (ORIENTATION = ROW )
DISTRIBUTE BY HASH (id,data_type)
PARTITION BY RANGE(pt)
(
        PARTITION P20200101000500 VALUES LESS THAN(20200101000500)
);