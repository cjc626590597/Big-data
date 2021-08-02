package com.suntek.algorithm.algorithm.association.fptreenonecpb;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhy
 * @date 2020-9-18 13:55
 */
public class STtree {
    //List<TreeNodeNoneCpb> branch;
    List<Integer> branch;
    int[] a;
    List<SubPath> path;
   // List<Integer> baseCount;
    Map<Integer, Integer> count = new HashMap<Integer, Integer>();

   /* public void setBranch(List<TreeNodeNoneCpb> branch) {
        this.branch = branch;
    }*/

    public List<SubPath> getPath() {
        return path;
    }

    public void setPath(List<SubPath> path) {
        this.path = path;
    }

    public void setA(int[] a) {
        this.a = a;
    }

    public int[] getA() {
        return a;
    }

    public void setBranch(List<Integer> branch) {
        this.branch = branch;
    }

   /* public void setBaseCount( List<Integer> baseCount) {
        this.baseCount = baseCount;
    }*/

    public void setCount(Map<Integer, Integer> count) {
        this.count = count;
    }

    /*public List<TreeNodeNoneCpb> getBranch() {
        return this.branch;
    }*/

    public List<Integer> getBranch() {
        return this.branch;
    }

   /* public List<Integer> getBaseCount() {
        return this.baseCount;
    }*/

    public Map<Integer, Integer> getCount() {
        return this.count;
    }
}
