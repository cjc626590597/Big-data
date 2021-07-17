package HOT100;

import java.util.LinkedList;

class Solution21 {
    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
//        ListNode head, tail;
//        if (l1 == null){
//            head = l2;
//        }else if (l2 == null){
//            head = l1;
//        }
//        if (l1.val <= l2.val){
//            head = tail = l1;
//            l1 = l1.next;
//        }else {
//            head = tail = l2;
//            l2 = l2.next;
//        }
//        while (l1 != null && l2 != null){
//            if (l1.val <= l2.val){
//                tail.next = l1;
//                l1 = l1.next;
//            }else {
//                tail.next = l2;
//                l2 = l2.next;
//            }
//            tail = tail.next;
//        }
//        tail.next = l1==null?l2:l1;
//        ListNode.printList(head);
//        return head;
        if (l1==null){
            return l2;
        }else if (l2==null){
            return l1;
        }else if (l1.val<l2.val){
            l1.next = mergeTwoLists(l1.next,l2);
            return l1;
        }else {
            l2.next = mergeTwoLists(l1,l2.next);
            return l2;
        }
    }
}

public class problem21{
    public static void main(String[] args) {
        int l1[] = new int[]{1,3,4};
        int l2[] = new int[]{1,2,4};
        ListNode list1 = new ListNode(l1[0]);
        ListNode list2 = new ListNode(l2[0]);
        for (int i = 1; i < l1.length; i++) {
            ListNode.addToTail(list1, l1[i]);
        }
        for (int i = 1; i < l2.length; i++) {
            ListNode.addToTail(list2, l2[i]);
        }
        ListNode.printList(new Solution21().mergeTwoLists(list1,list2));
    }
}