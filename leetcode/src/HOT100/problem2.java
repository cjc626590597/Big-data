package HOT100;

import java.util.List;

/**
 You are given two non-empty linked lists representing two non-negative integers. The digits are stored in reverse order, and each of their nodes contains a single digit. Add the two numbers and return the sum as a linked list.

 You may assume the two numbers do not contain any leading zero, except the number 0 itself.

 */

public class problem2 {
}

/**
 * Definition for singly-linked list.
 */

class ListNode {
    int val;
    ListNode next;
    ListNode() {}
    ListNode(int val) { this.val = val; }
    ListNode(int val, ListNode next) { this.val = val; this.next = next; }

    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode head=null, tail=null;
        int carry=0 ;
        int sum=0 ;
        while (l1!=null || l2!=null){
            int n1 = l1!=null? l1.val:0;
            int n2 = l2!=null? l2.val:0;
            sum = n1+n2+carry;
            if (head==null){
                head = tail = new ListNode(sum%10);
            }else {
                tail.next = new ListNode(sum%10);
                tail = tail.next;
            }
            carry = sum / 10;
            if (l1!=null){
                l1= l1.next;
            }
            if (l2!=null){
                l2= l2.next;
            }
        }
        if (carry>0){
            tail.next = new ListNode(carry);
        }
        return head;
    }

    static void addToTail(ListNode head, int val){
        ListNode node = new ListNode(val);
        if (head.val == 0){
            head = node;
        }else {
            ListNode temp = head;
            while (temp.next != null){
                temp = temp.next;
            }
            temp.next = node;
        }
    }

    static void printList(ListNode head){
        ListNode temp = head;
        while (temp != null){
            System.out.print(temp.val);
            temp = temp.next;
        }
    }
}

class Solution2 {
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode head=null, tail=null;
        int carry=0 ;
        int sum=0 ;
        while (l1!=null || l2!=null){
            int n1 = l1!=null? l1.val:0;
            int n2 = l2!=null? l2.val:0;
            sum = n1+n2+carry;
            if (head==null){
                head = tail = new ListNode(sum%10);
            }else {
                tail.next = new ListNode(sum%10);
                tail = tail.next;
            }
            carry = sum / 10;
            if (l1!=null){
                l1= l1.next;
            }
            if (l2!=null){
                l2= l2.next;
            }
        }
        if (carry>0){
                tail.next = new ListNode(carry);
        }
        return head;
    }

    void addToTail(ListNode head, int val){
        ListNode node = new ListNode(val);
        if (head.val == 0){
            head = node;
        }else {
            ListNode temp = head;
            while (temp.next != null){
                temp = temp.next;
            }
            temp.next = node;
        }
    }

    void printList(ListNode head){
        ListNode temp = head;
        while (temp != null){
            System.out.print(temp.val);
            temp = temp.next;
        }
    }

    public static void main(String[] args) {
        Solution2 solution2 = new Solution2();
        int l1[] = new int[]{1,5,};
        int l2[] = new int[]{1,5,9};
        ListNode list1 = new ListNode(l1[0]);
        ListNode list2 = new ListNode(l2[0]);
        for (int i = 1; i < l1.length; i++) {
            solution2.addToTail(list1, l1[i]);
        }
        for (int i = 1; i < l2.length; i++) {
            solution2.addToTail(list2, l2[i]);
        }
//        solution2.printList(list1);
//        System.out.println();
//        solution2.printList(list2);
        solution2.printList(solution2.addTwoNumbers(list1, list2));
    }
}