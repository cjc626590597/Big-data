package com.suntek.algorithm.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


@Description(
        name = "calCoord",
        value = "_FUNC_(lng_a,lat_a,lng_b,lat_b) - from the input lon1,lat1,lon2,lat2. "
                + "returns the value that is \" distance\" ",
        extended = "Example:\n"
                + " > SELECT _FUNC_(lng_a,lat_a,lng_b,lat_b) FROM src;"
)
public class CoordUtils extends UDF {
    // 地球半径
    private float R =  6371;

    public Double evaluate( Double lng_a,Double  lat_a,Double lng_b,Double lat_b) {
        if (lng_a.equals( lng_b) && lat_a.equals(lat_b)) {
               return 0.0;
        }

        Double lat1 = (Math.PI / 180) * lat_a;
        Double lat2 = (Math.PI / 180) * lat_b;
        // 经度
        Double lon1 = (Math.PI / 180) * lng_a;
        Double lon2 = (Math.PI / 180) * lng_b;
        Double d = Math.acos(Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1)) * R;
        return d*1000;
    }






}
