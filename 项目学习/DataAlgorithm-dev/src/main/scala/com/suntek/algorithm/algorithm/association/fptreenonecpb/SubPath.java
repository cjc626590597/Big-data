package com.suntek.algorithm.algorithm.association.fptreenonecpb;

import java.util.List;

/**
 * @author zhy
 * @date 2020-9-24 9:20
 */
public class SubPath{
    private List<TreeNodeNoneCpb> subPath;
    private int baseCount;

    public SubPath(){

    }

    public SubPath(List<TreeNodeNoneCpb> subPath, int baseCount){
        this.subPath = subPath;
        this.baseCount = baseCount;
    }

    public void setSubPath(List<TreeNodeNoneCpb> subPath) {
        this.subPath = subPath;
    }

    public List<TreeNodeNoneCpb> getSubPath() {
        return subPath;
    }

    public void setBaseCount(int baseCount) {
        this.baseCount = baseCount;
    }

    public int getBaseCount() {
        return baseCount;
    }
}
