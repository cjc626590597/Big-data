package com.suntek.algorithm.algorithm.association.fptreenonecpb;

/**
 * @author zhy
 * @date 2020-9-16 18:16
 */
public class TransformNode {
    private String item;
    private Integer index;
    private Double support;

    public Double getSupport() {
        return support;
    }

    public void setSupport(Double support) {
        this.support = support;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    private Integer count;

    public TransformNode(String item, Integer index, Integer count) {
        this.item = item;
        this.index = index;
        this.count = count;
    }


    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "item-"+ item + "; count-" + count + "; support-" + support;
    }
}
