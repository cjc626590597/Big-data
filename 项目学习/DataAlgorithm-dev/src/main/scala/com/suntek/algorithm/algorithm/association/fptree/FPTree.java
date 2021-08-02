package com.suntek.algorithm.algorithm.association.fptree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 *
 * @Description: FPTree强关联规则挖掘算法
 * @Author zhy
 * @Date Jun 23, 2016
 */
public class FPTree {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    /**频繁模式的最小支持数**/
    private double minSuportPercent;
    private int minSuport;
    private double maxPathLength = 30;
    /**关联规则的最小置信度**/
    private double confident;
    /**事务项的总数**/
    private int totalSize;
    /**存储每个频繁项及其对应的计数**/
    private Map<List<String>, Integer> frequentMap = new HashMap<List<String>, Integer>();
    /**关联规则中，哪些项可作为被推导的结果，默认情况下所有项都可以作为被推导的结果**/
    private Set<String> decideAttr = null;

    public double getMinSuport() {
        return this.minSuport;
    }
    public double getMinSuportPercent() {
        return this.minSuportPercent;
    }

    /**
     * 设置最小支持数
     *
     * @param minSuport
     */
    public void setMinSuport(int minSuport) {
        this.minSuport = minSuport;
    }

    public void setMinSuportPercent(double minSuportPercent) {
        this.minSuportPercent = minSuportPercent;
    }

    public double getConfident() {
        return confident;
    }

    /**
     * 设置最小置信度
     *
     * @param confident
     */
    public void setConfident(double confident) {
        this.confident = confident;
    }

    /**
     * 设置决策属性。如果要调用{@linkplain #readTransRocords(String[])}，需要在调用{@code readTransRocords}之后再调用{@code setDecideAttr}
     *
     * @param decideAttr
     */
    public void setDecideAttr(Set<String> decideAttr) {
        this.decideAttr = decideAttr;
    }

    /**
     * 获取频繁项集
     *
     * @return
     * @Description:
     */
    public Map<List<String>, Integer> getFrequentItems() {
        return frequentMap;
    }

    public Map<Integer, List<String>> getMaxFrequentItems() {
        Set<Entry<List<String>, Integer>> frequentItems = frequentMap.entrySet();
        if(frequentItems.size() == 0) return  new HashMap<Integer, List<String>>();
        Integer maxSize = 0;
        Integer maxValue = 0;
        Map<String, HashSet<String>> map = new HashMap<String, HashSet<String>>();
        for(Entry<List<String>, Integer> entry: frequentItems){
            List<String> keys = entry.getKey();
            HashSet keySet = new HashSet(keys);
            Integer value = entry.getValue();
            Integer size = keys.size();
            if(map.containsKey(size + "_" + value)){
                HashSet<String> set =  map.get(size + "_" + value);
                set.addAll(keySet);
            }else {
                map.put(size + "_" + value, keySet);
            }
            if(maxSize < size){
                maxSize = size;
                maxValue = value;
            }else if(maxSize == size){
                if(value > maxValue){
                    maxValue = value;
                }
            }
        }
        Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
        ret.put(maxValue, new ArrayList<String>(map.get(maxSize + "_" + maxValue)));
        return ret;
        //map.get(maxSize + "_" + maxValue).stream().distinct().collect(Collectors.toList());
    }

    public int getTotalSize() {
        return totalSize;
    }

    /**
     * 根据一条频繁模式得到若干关联规则
     *
     * @param list
     * @return
     */
    private List<StrongAssociationRule> getRules(List<String> list) {
        List<StrongAssociationRule> rect = new LinkedList<StrongAssociationRule>();
        if (list.size() > 1) {
            for (int i = 0; i < list.size(); i++) {
                String result = list.get(i);
                if (decideAttr.contains(result)) {
                    List<String> condition = new ArrayList<String>();
                    condition.addAll(list.subList(0, i));
                    condition.addAll(list.subList(i + 1, list.size()));
                    StrongAssociationRule rule = new StrongAssociationRule();
                    rule.condition = condition;
                    rule.result = result;
                    rect.add(rule);
                }
            }
        }
        return rect;
    }

    /**
     * 从若干个文件中读入Transaction Record，同时把所有项设置为decideAttr
     *
     * @param filenames
     * @return
     * @Description:
     */
    public List<List<String>> readTransRocords(String[] filenames) {
        Set<String> set = new HashSet<String>();
        List<List<String>> transaction = null;
        if (filenames.length > 0) {
            transaction = new LinkedList<List<String>>();
            for (String filename : filenames) {
                try {
                    FileReader fr = new FileReader(filename);
                    BufferedReader br = new BufferedReader(fr);
                    try {
                        String line = null;
                        // 一项事务占一行
                        while ((line = br.readLine()) != null) {
                            if (line.trim().length() > 0) {
                                // 每个item之间用","分隔
                                String[] str = line.split(",");
                                //每一项事务中的重复项需要排重
                                Set<String> record = new HashSet<String>();
                                for (String w : str) {
                                    record.add(w);
                                    set.add(w);
                                }
                                List<String> rl = new ArrayList<String>();
                                rl.addAll(record);
                                transaction.add(rl);
                            }
                        }
                    } finally {
                        br.close();
                    }
                } catch (IOException ex) {
                    System.out.println("Read transaction records failed." + ex.getMessage());
                    System.exit(1);
                }
            }
        }

        this.setDecideAttr(set);
        return transaction;
    }

    /**
     * 生成一个序列的各种子序列。（序列是有顺序的）
     *
     * @param residualPath
     * @param results
     */
    private void combine(LinkedList<TreeNode> residualPath, List<List<TreeNode>> results) {
        if (residualPath.size() > 0) {
            //如果residualPath太长，则会有太多的组合，内存会被耗尽的
            TreeNode head = residualPath.poll();
            List<List<TreeNode>> newResults = new ArrayList<List<TreeNode>>();
            for (List<TreeNode> list : results) {
                List<TreeNode> listCopy = new ArrayList<TreeNode>(list);
                newResults.add(listCopy);
            }

            for (List<TreeNode> newPath : newResults) {
                newPath.add(head);
            }
            results.addAll(newResults);
            List<TreeNode> list = new ArrayList<TreeNode>();
            list.add(head);
            results.add(list);
            combine(residualPath, results);
        }
    }

    private boolean isSingleBranch(TreeNode root) {
        boolean rect = true;
        while (root.getChildren() != null) {
            if (root.getChildren().size() > 1) {
                rect = false;
                break;
            }
            root = root.getChildren().get(0);
        }
        return rect;
    }

    /**
     * 计算事务集中每一项的频数
     *
     * @param transRecords
     * @return
     */
    private LinkedHashMap<String, Integer> getFrequency(List<List<String>> transRecords) {
        LinkedHashMap<String, Integer> frequentMap = new LinkedHashMap<String, Integer>();
        HashMap<String, Integer> countMap = new HashMap<String, Integer>();
        for (List<String> record : transRecords) {
            for (String item : record) {
                Integer cnt = countMap.get(item);
                if (cnt == null) {
                    cnt = new Integer(0);
                }
                countMap.put(item, ++cnt);
            }
        }
        // 删除支持度低于阈值的项
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            // 过滤掉不满足支持度的项
            double support =  entry.getValue() * 1.0 /transRecords.size() * 1.0;
            if (support >= minSuportPercent && entry.getValue() >= minSuport){
                frequentMap.put(entry.getKey(), entry.getValue());
            }
        }
        // 按照支持度降序排列
        List<Map.Entry<String, Integer>> mapList = new ArrayList<Map.Entry<String, Integer>>(frequentMap.entrySet());
        Collections.sort(mapList, new Comparator<Map.Entry<String, Integer>>() {// 降序排序
            @Override
            public int compare(Entry<String, Integer> v1,
                               Entry<String, Integer> v2) {
                return v2.getValue() - v1.getValue();
            }
        });
        countMap.clear();
        frequentMap.clear();// 清空，以便保持有序的键值对
        for (Map.Entry<String, Integer> entry : mapList) {
            frequentMap.put(entry.getKey(), entry.getValue());
        }
        return frequentMap;
    }

    /**
     * 根据事务集合构建FPTree
     *
     * @param transRecords
     * @Description:
     */
    public void buildFPTree(List<List<String>> transRecords) {
        long curTimeMillis = System.currentTimeMillis();
        totalSize = transRecords.size();
        FPGrowth(transRecords, null);
    }

    /**
     * FP树递归生长，从而得到所有的频繁模式
     *
     * @param cpb  条件模式基
     * @param postModel   后缀模式
     */
    private void FPGrowth(List<List<String>> cpb, LinkedList<String> postModel) {
        LinkedHashMap<String, Integer> freqMap = getFrequency(cpb);
        LinkedHashMap<String, TreeNode> headers = new LinkedHashMap<String, TreeNode>();
        for (Entry<String, Integer> entry : freqMap.entrySet()) {
            String name = entry.getKey();
            int cnt = entry.getValue();
            TreeNode node = new TreeNode(name);
            node.setCount(cnt);
            headers.put(name, node);
        }

        TreeNode treeRoot = buildSubTree(cpb, freqMap, headers);
        //如果只剩下虚根节点，则递归结束
        if ((treeRoot.getChildren() == null) || (treeRoot.getChildren().size() == 0)) {
            return;
        }
        //如果树是单枝的，则直接把“路径的各种组合+后缀模式”添加到频繁模式集中。这个技巧是可选的，
        // 即跳过此步进入下一轮递归也可以得到正确的结果
        if (isSingleBranch(treeRoot)) {
            LinkedList<TreeNode> path = new LinkedList<TreeNode>();
            TreeNode currNode = treeRoot;
            while (currNode.getChildren() != null) {
                currNode = currNode.getChildren().get(0);
                path.add(currNode);
            }
            //调用combine时path不宜过长，否则会OutOfMemory
            if (path.size() <= maxPathLength) {
                List<List<TreeNode>> results = new ArrayList<List<TreeNode>>();
                combine(path, results);
                for (List<TreeNode> list : results) {
                    int cnt = 0;
                    List<String> rule = new ArrayList<String>();
                    for (TreeNode node : list) {
                        rule.add(node.getName());
                        cnt = node.getCount();//cnt最FPTree叶节点的计数
                    }
                    if (postModel != null) {
                        rule.addAll(postModel);
                    }
                    if(cnt * 1.0 / totalSize * 1.0 >= minSuportPercent && cnt >= minSuport){
                        frequentMap.put(rule, cnt);
                    }

                }
                return;
            } else {
                logger.info("length of path is too long: " + path.size());
            }
        }

        for (TreeNode header : headers.values()) {
            List<String> rule = new ArrayList<String>();
            rule.add(header.getName());
            if (postModel != null) {
                rule.addAll(postModel);
            }
            //表头项+后缀模式  构成一条频繁模式（频繁模式内部也是按照F1排序的），频繁度为表头项的计数
            if(header.getCount() * 1.0 / totalSize * 1.0 >= minSuportPercent && header.getCount() >= minSuport){
                frequentMap.put(rule, header.getCount());
            }

            //新的后缀模式：表头项+上一次的后缀模式（注意保持顺序，始终按F1的顺序排列）
            LinkedList<String> newPostPattern = new LinkedList<String>();
            newPostPattern.add(header.getName());
            if (postModel != null) {
                newPostPattern.addAll(postModel);
            }
            //新的条件模式基
            List<List<String>> newCPB = new LinkedList<List<String>>();
            TreeNode nextNode = header;
            while ((nextNode = nextNode.getNextHomonym()) != null) {
                int counter = nextNode.getCount();
                //获得从虚根节点（不包括虚根节点）到当前节点（不包括当前节点）的路径，即一条条件模式基。注意保持顺序：你节点在前，子节点在后，即始终保持频率高的在前
                LinkedList<String> path = new LinkedList<String>();
                TreeNode parent = nextNode;
                while ((parent = parent.getParent()).getName() != null) {//虚根节点的name为null
                    path.push(parent.getName());//往表头插入
                }
                //事务要重复添加counter次
                while (counter-- > 0) {
                    newCPB.add(path);
                }
            }
            FPGrowth(newCPB, newPostPattern);
        }
    }

    /**
     * 将line数组里id按照frequentMap的值得降序排序
     */
    private LinkedList<String> getOrderLine(List<String> line,
                                      LinkedHashMap<String, Integer> frequentMap) {
        LinkedList<String> ret = new LinkedList<String>();
        for (String key : frequentMap.keySet()) {
            if (line.indexOf(key) != -1) {
                ret.add(key);
            }
        }
        return ret;
    }
    /**
     * 把所有事务插入到一个FP树当中
     *
     * @param transRecords
     * @return
     */
    private TreeNode buildSubTree(List<List<String>> transRecords,
                                  final LinkedHashMap<String, Integer> freqMap,
                                  final LinkedHashMap<String, TreeNode> headers) {
        TreeNode root = new TreeNode();//虚根节点
        for (List<String> transRecord : transRecords) {
            LinkedList<String> record = getOrderLine(transRecord, freqMap);
            TreeNode subTreeRoot = root;
            TreeNode tmpRoot = null;
            if (root.getChildren() != null) {
                //延已有的分支，令各节点计数加1
                while (!record.isEmpty()
                        && (tmpRoot = subTreeRoot.findChild(record.peek())) != null) {
                    tmpRoot.countIncrement(1);
                    subTreeRoot = tmpRoot;
                    record.poll();
                }
            }
            //长出新的节点
            addNodes(subTreeRoot, record, headers);
        }
        return root;
    }

    /**
     * 往特定的节点下插入一串后代节点，同时维护表头项到同名节点的链表指针
     *
     * @param ancestor
     * @param record
     * @param headers
     */
    private void addNodes(TreeNode ancestor, LinkedList<String> record,
                          final Map<String, TreeNode> headers) {
        while (!record.isEmpty()) {
            String item = (String) record.poll();
            //单个项的出现频数必须大于最小支持数，否则不允许插入FP树。达到最小支持度的项都在headers中。每一次递归根据条件模式基本建立新的FPTree时，把要把频数低于minSuport的排除在外，这也正是FPTree比穷举法快的真正原因
            if (headers.containsKey(item)) {
                TreeNode leafnode = new TreeNode(item);
                leafnode.setCount(1);
                leafnode.setParent(ancestor);
                ancestor.addChild(leafnode);

                TreeNode header = headers.get(item);
                TreeNode tail=header.getTail();
                if(tail!=null){
                    tail.setNextHomonym(leafnode);
                }else{
                    header.setNextHomonym(leafnode);
                }
                header.setTail(leafnode);
                addNodes(leafnode, record, headers);
            }
        }
    }

    /**
     * 获取所有的强规则
     *
     * @return
     */
    public List<StrongAssociationRule> getAssociateRule() {
        assert totalSize > 0;
        List<StrongAssociationRule> rect = new ArrayList<StrongAssociationRule>();
        //遍历所有频繁模式
        for (Entry<List<String>, Integer> entry : frequentMap.entrySet()) {
            List<String> items = entry.getKey();
            int count1 = entry.getValue();
            //一条频繁模式可以生成很多关联规则
            List<StrongAssociationRule> rules = getRules(items);
            //计算每一条关联规则的支持度和置信度
            for (StrongAssociationRule rule : rules) {
                if (frequentMap.containsKey(rule.condition)) {
                    int count2 = frequentMap.get(rule.condition);
                    double confidence = 1.0 * count1 / count2;
                    if (confidence >= this.confident) {
                        rule.support = count1;
                        rule.confidence = confidence;
                        rect.add(rule);
                    }
                } else {
                    System.err.println(rule.condition + " is not a frequent pattern, however "
                            + items + " is a frequent pattern");
                }
            }
        }
        return rect;
    }

    
}

