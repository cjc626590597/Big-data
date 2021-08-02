package com.suntek.algorithm.algorithm.association.fptreenonecpb;

import org.apache.spark.sql.sources.In;

import java.util.*;

/**
 * FPTree改进算法,不生成条件树
 * @author zhy
 * @date 2020-9-16 17:55
 *
 */
public class FPTreeNoneCpb {
    private int n = 0;
    private TransformTable transformTable = new TransformTable();
    private Map<Integer, TransformTableMax> transformTableMax = new HashMap<>(); // 最大频繁集
    private LinkedHashMap<Integer, TreeNodeNoneCpb> headers = new LinkedHashMap<>();
    private List<TransformNode> items = new ArrayList<>();
    private Integer minCount = 2;
    private double minSupport = 0.3;
    private Map<Integer, TransformNode> frequentMap = new HashMap<>();
    private Long timeOut = 30000L * 1000000000;
    private Boolean isFindMaxFrequentSet = false; // 是否需要获取最大频繁集

    public Map<Integer, TransformTableMax> getTransformTableMax() {
        return transformTableMax;
    }

    public void setIsFindMaxFrequentSet(Boolean isFindMaxFrequentSet) {
        this.isFindMaxFrequentSet = isFindMaxFrequentSet;
    }

    public TransformTable  getTransformTable() {
        return transformTable;
    }

    public void setN(int n){
        this.n = n;
    }

    public void setMinCount(int minCount){
        this.minCount = minCount;
    }

    public void setMinSupport(double minSupport){
        this.minSupport = minSupport;
    }

    private void addNode(String item, Integer index, Integer count){
        TransformNode node = new TransformNode(item, index, count);
        items.add(node);
    }

    private Integer getIndex(String item){
        for(TransformNode node: items){
            if(item.equals(node.getItem())){
                return node.getIndex();
            }
        }
        return -1;
    }

    /**
     * 初始化项
     * @param transRecords
     */
    private void genItems(List<List<String>> transRecords) {
        HashMap<String, Integer> countMap = new HashMap<>();
        // 得到每个项的支持度计数
        for (List<String> record : transRecords) {
            for (String item : record) {
                Integer cnt = countMap.get(item);
                if (cnt == null) {
                    cnt = new Integer(0);
                }
                countMap.put(item, ++cnt);
            }
        }
        LinkedHashMap<String, Integer> tmpMap = new LinkedHashMap<>();
        // 删除支持度低于阈值的项
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            // 频繁模式的最小支持数
            double support = entry.getValue() * 1.0 / n * 1.0;
            if (entry.getValue() >= minCount && support >= minSupport){
                tmpMap.put(entry.getKey(), entry.getValue());
            }
        }
        // 将项按支持度计数降序排序 ,得到每个项的序号 ,并生成项2序转换表和序2项转换表
        List<Map.Entry<String, Integer>> mapList = new ArrayList<>(tmpMap.entrySet());
        // 降序排序
        mapList.sort((v1, v2) -> {
            if (!v2.getValue().equals(v1.getValue())) {
                return v2.getValue() - v1.getValue();
            } else {
                return v2.getKey().compareTo(v1.getKey());
            }
        });
        countMap.clear();
        tmpMap.clear();
        for(int i = 0; i< mapList.size(); i ++){
            Map.Entry<String, Integer> entry = mapList.get(i);
            addNode(entry.getKey(), i, entry.getValue());
        }
    }

    /**
     * 初始化表头
     */
    public void getHeaders(TreeNodeNoneCpb root) {
        if(root.getNext() != null || root.getAhead() != null){
            for (TransformNode item: items) {
                Integer index = item.getIndex();
                int cnt = item.getCount();
                TreeNodeNoneCpb node = new TreeNodeNoneCpb(index);
                node.setCount(cnt);
                headers.put(index, node);
            }
            getLinkTable(root.getAhead(), null);
        }
    }

    /**
     * 构建树
     */
    private STtree buildSTTree(List<TreeNodeNoneCpb> st, List<SubPath> path){
        TreeNodeNoneCpb nodeKi = st.get(st.size() - 1);
        int[] a1 = new int[nodeKi.getIndex() + 1];
        List<SubPath> curPaths = new ArrayList<>();
        TreeNodeNoneCpb minNode = st.get(st.size() - 1);
        for(int j = 0; j< path.size(); j++){
            SubPath subPath = path.get(j);
            List<TreeNodeNoneCpb> subPathList = subPath.getSubPath();
            boolean flag = false;
            int maxI = -1;
            for(int k = 0; k< subPathList.size(); k++){
                if(minNode.getIndex().equals(subPathList.get(k).getIndex())){
                    flag = true;
                    maxI = k;
                    break;
                }
            }
            if(flag){
                SubPath curPath = new SubPath();
                curPath.setSubPath(subPathList.subList(maxI, subPathList.size()));
                curPath.setBaseCount(subPath.getBaseCount());
                int k = maxI + 1;
                while (k < subPathList.size()){
                    int t = a1[subPathList.get(k).getIndex()];
                    a1[subPathList.get(k).getIndex()] = t + subPath.getBaseCount();
                    k = k + 1;
                }
                curPath.setBaseCount(subPath.getBaseCount());
                curPaths.add(curPath);
            }
        }
        Map<Integer, Integer> count = new HashMap<>();
        List<Integer> branch = new ArrayList<>();
        for(int i = 0; i< a1.length; i ++){
            if(a1[i] != 0){
                branch.add(i);
                count.put(i, a1[i]);
            }
        }
        if(count.size() <= 0) return null;
        STtree stTree = new STtree();
        stTree.setPath(curPaths);
        stTree.setA(a1);
        stTree.setBranch(branch);
        stTree.setCount(count);
        return stTree;
    }

    /**
     * 获取二项集，这里有个特殊处理，由于支持度很低的时候二项集合可能会很多，
     * 因此获取的集合生成二项集的时候以支持次数最高项做过滤，获取和支持次数最高项在同一集合内的项
     */
    private void getFrequentMap(){
        TreeNodeNoneCpb node = this.headers.get(0);
        boolean flag = false;
        for(Integer it: frequentMap.keySet()){
            TransformNode tmpNode = frequentMap.get(it);
            if(tmpNode.getIndex() == node.getIndex()){
                flag = true;
                break;
            }
        }
        if(flag){
            Map<Integer, TransformNode> map = transformTable.getFrequentMap();
            for(Integer it: frequentMap.keySet()){
                TransformNode tmpNode = frequentMap.get(it);
                map.put(tmpNode.getIndex(), tmpNode);
            }
            transformTable.setN(n);
        }
    }

    /**
     * 获取最大频繁项集
     * @param count
     * @param support
     */
    private void getMaxFrequentMap(double count, double support){
        if(!isFindMaxFrequentSet) return;
        int setN = frequentMap.size();
        if(transformTableMax.size() == 0){
            TransformTableMax t = new TransformTableMax(count, support, frequentMap);
            transformTableMax.put(setN, t);
        }else{
            List<Integer> keyList = new ArrayList<>(transformTableMax.keySet());
            Collections.sort(keyList);
            Integer maxKey = keyList.get(keyList.size() - 1);
            Integer key = maxKey;
            List<Integer> curKey = new ArrayList<>();
            Boolean insertFlag = false;
            Boolean deleteFlag = false;
            while (key>0 && key >= setN){
                if(transformTableMax.containsKey(key)){
                    if(key >= setN){
                        curKey = transformTableMax.get(key).findNode(frequentMap, true);
                        if(curKey.size()>0) break;
                    }
                }
                key --;
            }
            if(curKey.size()<= 0){
                insertFlag = true;
                if(transformTableMax.containsKey(setN)){
                    transformTableMax.get(setN).addFrequentMap(frequentMap);
                }else {
                    TransformTableMax t = new TransformTableMax(count, support, frequentMap);
                    transformTableMax.put(setN, t );
                }
            }
            key = setN - 1;
            while (key>0){
                if(transformTableMax.containsKey(key)){
                    if(key < setN){
                        TransformTableMax t = transformTableMax.get(key);
                        curKey = t.findNode(frequentMap, false);
                        List<Map<Integer, TransformNode>> list = t.getFrequentList();
                        List<Map<Integer, TransformNode>> subList = new ArrayList<>();
                        if(curKey.size() == 0){
                            subList = list;
                        }else{
                            for(int k = 0; k < list.size(); k++){
                                if(!curKey.contains(Integer.valueOf(k))){
                                    subList.add(list.get(k));
                                }
                            }
                        }
                        t.setFrequentList(subList);
                    }
                }
                key --;
            }
            if(!insertFlag && deleteFlag){
                if(transformTableMax.containsKey(setN)){
                    transformTableMax.get(setN).addFrequentMap(frequentMap);
                }else {
                    TransformTableMax t = new TransformTableMax(count, support, frequentMap);
                    transformTableMax.put(setN, t );
                }
            }
        }
    }

    /**
     * fptree的挖掘迭代
     */
    private Boolean mine(List<TreeNodeNoneCpb> st, STtree stTree, int length, Long startTimeStamp, String id){
        Long currentTimeStamp = System.currentTimeMillis();
        if(currentTimeStamp - startTimeStamp >= timeOut){
            return false;
        }
        if(stTree.getPath().size() == 1){
            genOnlyPath(stTree, length);
        }else {
            int index = st.get(st.size() - 1).getIndex() - 1;
            for(int i = index ; i >= 0; i--){
                if(!stTree.getCount().containsKey(i)) continue;
                double support = stTree.getCount().get(i) * 1.0 / n * 1.0;
                double count = stTree.getCount().get(i);
                if(count >= minCount && support >= minSupport ) {
                    frequentMap.put(++ length, items.get(i));
                    getMaxFrequentMap(count, support);
                    getFrequentMap();
                    TreeNodeNoneCpb addNode = headers.get(i).getNext();
                    st.add(addNode);
                    STtree stTree1 = buildSTTree(st, stTree.getPath());
                    if(stTree1!= null && stTree1.getCount().size() >0){
                        mine(st, stTree1, length, startTimeStamp, id);
                    }
                    st.remove(st.size() - 1);
                    frequentMap.remove(length --);
                }
            }
        }
        return true;
    }

    /**
     * 初始化STtree
     */
    private STtree initSTTree(TreeNodeNoneCpb node){
        int[] a = new int[node.getIndex() + 1];
        TreeNodeNoneCpb tmp = node;
        // 先找到序号最大的节点对应的所有路径
        List<SubPath> path = new ArrayList<>();
        while (tmp != null && tmp.getIndex().equals(node.getIndex())){
            List<TreeNodeNoneCpb> subPath = new ArrayList<>();
            TreeNodeNoneCpb curNode = tmp;
            while (curNode != null){
                subPath.add(curNode);
                if(tmp.getIndex() != curNode.getIndex()){
                    int s = a[curNode.getIndex()];
                    a[curNode.getIndex()] = s + tmp.getCount();
                }
                curNode = curNode.getAhead();
            }
            SubPath subPathNode = new SubPath(subPath, tmp.getCount());
            path.add(subPathNode);
            tmp = tmp.getNext();
        }

        Map<Integer, Integer> count = new HashMap<>();
        List<Integer> branch = new ArrayList<>();
        for(int i = 0; i< a.length; i ++){
            if(a[i] != 0){
                branch.add(i);
                count.put(i, a[i]);
            }
        }

        STtree stTree = null;
        if(count.size() > 0){
            stTree = new STtree();
            stTree.setA(a);
            stTree.setPath(path);
            stTree.setBranch(branch);
            stTree.setCount(count);
        }
        return stTree;
    }

    /**
     * 只有一条路径特殊处理
     */
    private void genOnlyPath(STtree stTree, int length){
        int baseCount = stTree.getPath().get(0).getBaseCount();
        List<TreeNodeNoneCpb> path = stTree.getPath().get(0).getSubPath();
        double support = baseCount * 1.0 / n * 1.0;
        if(baseCount >= minCount && support >= minSupport ) {
            //frequentMap.clear();
            for(int i = 1; i< path.size(); i ++){
                TreeNodeNoneCpb nodeT = path.get(i);
                Iterator<TransformNode> it = frequentMap.values().iterator();
                Boolean flag = true;
                while (it.hasNext()){
                    TransformNode t = it.next();
                    if(nodeT.getIndex() == t.getIndex()){
                        flag = false;
                        break;
                    }
                }
                if(flag){
                    frequentMap.put(++ length, items.get(nodeT.getIndex()));
                }
            }
            getMaxFrequentMap(baseCount, support);
            getFrequentMap();
            frequentMap.clear();
        }
    }

    /**
     * FPTree 挖掘
     */
    public void fPMining(String id, Long currentTimeStamp){
        Integer max = headers.size() - 1;
        int length = 0;
        boolean flag = true;
        for(int i = max; i>= 0; i--){
            TreeNodeNoneCpb node = headers.get(i).getNext();
            frequentMap.put(++length, items.get(i));
            List<TreeNodeNoneCpb> st = new ArrayList<>();
            st.add(node);
            STtree stTree = initSTTree(node);
            if(stTree != null){
                if(stTree.getPath().size() == 1){
                    genOnlyPath(stTree, length);
                    frequentMap.clear();
                }else {
                    flag = mine(st, stTree, length, currentTimeStamp, id);
                }
            }
            frequentMap.put(length--, null);
            if(!flag){
                frequentMap.clear();
                int k = 0;
                for(TransformNode nodeT: this.items){
                    if(k > 10) break;
                    double support = nodeT.getCount()* 1.0 / n * 1.0;
                    if(nodeT.getCount() >= minCount && support >= minSupport ) {
                        getMaxFrequentMap(nodeT.getCount(), support);
                        getFrequentMap();
                    }
                    k ++;
                }
                break;
            }
        }
    }


    /**
     * 构建链表
     */
    private void getLinkTable(TreeNodeNoneCpb root,
                              TreeNodeNoneCpb parent){
        if(root == null) return;
        if(root.getIndex() == null) return;
        TreeNodeNoneCpb node = headers.get(root.getIndex());
        while (node.getNext() != null && node.getNext().getIndex().equals(root.getIndex())){
            node = node.getNext();
        }
        node.setNext(root);
        TreeNodeNoneCpb leftNode = root.getAhead();
        getLinkTable(leftNode, root);
        TreeNodeNoneCpb sis = root.getNext();
        getLinkTable(sis, parent);
        root.setAhead(parent);
    }

    /**
     * 将line数组里id按照index升序排列
     */
    private LinkedList<Integer> getOrderLine(List<String> line) {
        LinkedList<Integer> ret = new LinkedList<>();
        for (String item: line) {
            Integer index = getIndex(item);
            if(index != -1){
                ret.add(index);
            }
        }
        // 升序排序
        Collections.sort(ret, (v1, v2) -> v1 - v2);
        return ret;
    }

    /**
     * 生成FPtree
     */
    public TreeNodeNoneCpb buildTree(List<List<String>> transRecords) {
        genItems(transRecords);
        TreeNodeNoneCpb root = new TreeNodeNoneCpb();//虚根节点
        for (List<String> transRecord : transRecords) {
            LinkedList<Integer> record = getOrderLine(transRecord);
            if(record.size() > 0){
                TreeNodeNoneCpb subTreeRoot = root;
                TreeNodeNoneCpb tmpRoot;
                if(root.getAhead() != null){
                    while (!record.isEmpty() && (tmpRoot = subTreeRoot.findChild(record.peek()))  != null ){
                        tmpRoot.countIncrement(1);
                        subTreeRoot = tmpRoot;
                        record.poll();
                    }
                }
                //长出新的节点
                addNodes(subTreeRoot, record);
            }

        }
        return root;
    }

    /**
     * 增加叶子
     */
    private void addNodes(TreeNodeNoneCpb ancestor,
                          LinkedList<Integer> record) {
        while (!record.isEmpty()) {
            Integer index = record.poll();
            TreeNodeNoneCpb leafnode = new TreeNodeNoneCpb(index);
            leafnode.setCount(1);
            if(ancestor.getAhead() == null){
                ancestor.setAhead(leafnode);
            }else {
                TreeNodeNoneCpb tmp = ancestor.getAhead();
                while (tmp.getNext() != null){
                    tmp = tmp.getNext();
                }
                tmp.setNext(leafnode);
            }
            addNodes(leafnode, record);
        }
    }
}


