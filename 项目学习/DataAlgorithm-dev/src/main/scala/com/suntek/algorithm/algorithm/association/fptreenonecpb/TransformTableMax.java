package com.suntek.algorithm.algorithm.association.fptreenonecpb;

import java.util.*;

/**
 * @author zhy
 * @date 2020-11-27 16:36
 */
public class TransformTableMax {
    private List<Map<Integer, TransformNode>> frequentList = new ArrayList<>(); //频繁项列表
    private Double support = 0.0D;
    private double count = 0.0D;

    public TransformTableMax(){

    }
    public TransformTableMax(double count, double support, Map<Integer, TransformNode> frequentMap){
        this.support = support;
        this.count = count;
        this.addFrequentMap(frequentMap);
    }


    public List<Integer> findNode(Map<Integer, TransformNode> map, Boolean findF){
        List<Integer> ret = new ArrayList<>();
        if(frequentList.size() == 0) return  new ArrayList<>();
        if(findF){
            // findF为true，查找map中的值是不是能全部在frequentList中找到
            int i;
            Boolean flag = true;
            for(i = 0; i< frequentList.size(); i++){
                flag = true;
                Map<Integer, TransformNode> curMap = frequentList.get(i);
                Iterator<Integer> it = map.keySet().iterator();
                while (it.hasNext()){
                    Integer id = it.next();
                    Integer index = map.get(id).getIndex();
                    if(!curMap.containsKey(index)){
                        flag = false;
                        break;
                    }
                }
                if(flag) break;
            }
            if(flag) {
                ret.add(i);
                return ret;
            }
            return  new ArrayList<>();
        }else{
            // findF为false，查找frequentList中的每个map有哪几个能在map中都找到
            int i;
            Boolean flag = true;
            ArrayList<Integer> list = new ArrayList<>();
            Iterator<TransformNode> mapV = map.values().iterator();
            while (mapV.hasNext()){
                list.add(mapV.next().getIndex());
            }
            for(i = 0; i< frequentList.size(); i++){
                flag = true;
                Map<Integer, TransformNode> curMap = frequentList.get(i);
                Iterator<Integer> it = curMap.keySet().iterator();
                while (it.hasNext()){
                    Integer index = it.next();
                    if(-1 == list.indexOf(index)){
                        flag = false;
                        break;
                    }
                }
                if(flag) ret.add(i);
            }
        }
        return ret;
    }

    public List<Map<Integer, TransformNode>> getFrequentList() {
        return frequentList;
    }

    public void  addFrequentMap(Map<Integer, TransformNode> frequentMap){
        Map<Integer, TransformNode> map = new HashMap<>();
        Iterator<TransformNode> it = frequentMap.values().iterator();
        while (it.hasNext()){
            TransformNode node = it.next();
            map.put(node.getIndex(), node);
        }
        this.frequentList.add(map);
    }

    public void setFrequentList(List<Map<Integer, TransformNode>> frequentList) {
        this.frequentList = frequentList;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public Double getSupport() {
        return support;
    }

    public void setSupport(Double support) {
        this.support = support;
    }
}
