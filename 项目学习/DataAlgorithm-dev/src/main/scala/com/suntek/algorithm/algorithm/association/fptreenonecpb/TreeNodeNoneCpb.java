package com.suntek.algorithm.algorithm.association.fptreenonecpb;

/**
 * @author zhy
 * @date 2020-9-16 18:29
 */
public class TreeNodeNoneCpb {
    private Integer index; // 项的序号
    private int count; //频数
    private TreeNodeNoneCpb ahead; // 指向最左子女结点
    private TreeNodeNoneCpb next; // 指向右兄弟结点或结点链中下一 结点的指针

    @Override
    public String toString() {
        if(index == null) return "";
        return index.toString();
    }

    public TreeNodeNoneCpb() {
    }

    public TreeNodeNoneCpb(Integer index) {
        this.index = index;
    }

    public Integer getIndex() {
        return this.index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public TreeNodeNoneCpb getAhead() {
        return this.ahead;
    }

    public void setAhead(TreeNodeNoneCpb ahead) {
        this.ahead = ahead;
    }

    public TreeNodeNoneCpb getNext() {
        return this.next;
    }

    public void setNext(TreeNodeNoneCpb next) {
        this.next = next;
    }

    public void countIncrement(int n) {
        this.count += n;
    }

    public TreeNodeNoneCpb findChild(Integer index){
        if(ahead == null) return null;
        TreeNodeNoneCpb node = ahead;
        while(node != null && node.getIndex() != index){
            node = node.next;
        }
        return node;
    }

}
