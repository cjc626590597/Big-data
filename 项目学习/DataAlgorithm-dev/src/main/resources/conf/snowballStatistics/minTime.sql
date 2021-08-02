select min (t) from ( select min (CREATE_TIME) t from ARCHIVE_VEHICLE_DETECT_RL where length (toString(CREATE_TIME))>=14 union all select min(CREATE_TIME)t from ARCHIVE_CODE_DETECT_RL where length (toString(CREATE_TIME))>=14 union all select  min(CREATE_TIME) t from HIVE_FACE_DETECT_RL where length (toString(CREATE_TIME))>=14 ) s where t > 0;

