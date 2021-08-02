package com.suntek.algorithm.algorithm.association.fptreenonecpb;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhy
 * @date 2020-9-16 18:09
 */
public class TransformTable {
    private Map<Integer, TransformNode> frequentMap = new HashMap<>(); //频繁项列表
    private double n = 0.0D;
    private double support = 0.0D;
    private double count = 0.0D;

    public TransformTable(){

    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public void setSupport(double support) {
        this.support = support;
    }

    public double getSupport() {
        return support;
    }

    public void setN(double n) {
        this.n = n;
    }

    public double getN() {
        return this.n;
    }

    public TransformTable(Map<Integer, TransformNode> frequentMap){
        this.frequentMap = frequentMap;
    }

    public void addFrequentNode(Map<Integer, TransformNode> node) {
        for(Map.Entry<Integer, TransformNode> entry: node.entrySet()){
            TransformNode v = entry.getValue();
            this.frequentMap.put(v.getIndex(), v);
        }

    }

    public void setFrequentMap(Map<Integer, TransformNode> frequentMap) {
        this.frequentMap = frequentMap;
    }

    public Map<Integer, TransformNode> getFrequentMap() {
        return frequentMap;
    }

    public String toString(){
        String ret = "";
        for(Map.Entry<Integer, TransformNode> entry : frequentMap.entrySet()){
            ret = ret + "_" + entry.getValue().toString();
        }
        return ret;
    }
}
