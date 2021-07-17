package HOT100;


import sun.security.util.Length;

import java.util.*;

class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;

    TreeNode() {
    }

    TreeNode(int val) {
        this.val = val;
    }

    TreeNode(int val, TreeNode left, TreeNode right) {
        this.val = val;
        this.left = left;
        this.right = right;
    }

    public void preorderPrint(TreeNode treeNode){
        if (treeNode!=null) {
            System.out.print(treeNode.val+" ");
            preorderPrint(treeNode.left);
            preorderPrint(treeNode.right);
        }
    }

    public void inorderPrint(TreeNode treeNode){
        if (treeNode!=null) {
            inorderPrint(treeNode.left);
            System.out.print(treeNode.val+" ");
            inorderPrint(treeNode.right);
        }
    }

}


class TreeTest {
    TreeNode root;
    String[] partTree;
    public String[] intialInput(String s) {
        String s1=s.substring(1,s.length()-1);
        partTree=s1.split(",");
        return partTree;
    }

    public TreeNode createNode(TreeNode rot,int index) {				//传入root给rot后，由于rot会new一下，从而指向别的地方，
        if(index>= partTree.length ) {						//从而root实际指向位置不变，所以返回值类型为TreeNode
            return null;
        }
        if(partTree[index].equals("null")  ) {					//equals判断，而不是==
            return null;
        }
        rot=new TreeNode(Integer.parseInt(partTree[index]));
        rot.left=createNode(rot.left,2*index+1);
        rot.right=createNode(rot.right,2*index+2);
        return rot;
    }

    public TreeNode createTree(String s) {
        partTree=intialInput(s);
        root=createNode(root,0);
        return root;
    }

    public TreeNode getNode(TreeNode root, int target){
        Deque<TreeNode> deque = new LinkedList<TreeNode>();
        while (root!=null || !deque.isEmpty()){
            while (root!=null){
                deque.push(root);
                root = root.left;
            }
            root = deque.pop();
            if(root.val==target) return root;
            root = root.right;
        }
        return null;
    }
}

class Solution94 {
    //递归
//    public List<Integer> inorderTraversal(TreeNode root) {
//        List<Integer> list = new ArrayList<Integer>();
//        inorderPrint(root, list);
//        return list;
//    }
//
//    public void inorderPrint(TreeNode treeNode, List<Integer> list){
//        if (treeNode!=null) {
//            inorderPrint(treeNode.left, list);
//            list.add(treeNode.val);
//            inorderPrint(treeNode.right, list);
//        }
//    }

    //压栈
        public List<Integer> inorderTraversal(TreeNode root) {
        List<Integer> list = new ArrayList<Integer>();
        Deque<TreeNode> deque = new LinkedList<TreeNode>();
        while (root!=null || !deque.isEmpty()){
            while (root!=null){
                deque.push(root);
                root = root.left;
            }
            root = deque.pop();
            list.add(root.val);
            root = root.right;
        }
        return list;
    }

    public static List<List<Integer>> levelOrder(TreeNode root) {
        Deque<TreeNode> deque = new LinkedList<>();
        List<List<Integer>> list= new ArrayList<>();
        deque.add(root);
        while (!deque.isEmpty()){
            int size = deque.size();
            List<Integer> level = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                TreeNode temp = deque.pop();
                level.add(temp.val);
                System.out.print(temp.val);
                if (temp.left != null){
                    deque.add(temp.left);
                }
                if (temp.right != null){
                    deque.add(temp.right);
                }
            }
            list.add(level);
        }
        Queue<String> queue = new LinkedList<>();
        return list;
    }

}

public class problem94{

    public static void main(String[] args) {
        TreeNode root = new TreeTest().createTree("[2,null,4,null,null,10,8,null,null,null,null,null,null,4]");
        System.out.println(new Solution().goodNodes(root));
    }
    static class Solution {
        public int goodNodes(TreeNode root) {
            List<Integer> list = new LinkedList<>();
            return goodNodes(root, list);
        }

        public int goodNodes(TreeNode root, List<Integer> list) {
            if(root == null) return 0;
            list.add(root.val);
            int flag = 1;
            for(Integer i:list){
                if(i > root.val){
                    flag = 0;
                    System.out.println(i + ">" + root.val);
                }
            }
            List<String> list1 = new ArrayList<>(100);
            return flag + goodNodes(root.left,list) + goodNodes(root.right,list);
        }
    }

}