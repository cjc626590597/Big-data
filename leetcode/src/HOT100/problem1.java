package HOT100;

import junit.extensions.TestSetup;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Given an array of integers nums and an integer target, return indices of the two numbers such that they add up to target.
 *
 * You may assume that each input would have exactly one solution, and you may not use the same element twice.
 *
 * You can return the answer in any order.
 */

class Solution1 {
    public int[] twoSum(int[] nums, int target) {
        int len = nums.length;
        Map<Integer, Integer> hashtable = new HashMap<>(len-1);
        for (int i = 0; i < len; i++) {
            if (hashtable.containsKey(target-nums[i])){
                return new int[]{hashtable.get(target-nums[i]), i};
            }
            hashtable.put(nums[i], i);
        }
        return null;
    }
}

class test{
    public static void main(String[] args) {
//        int nums[] = new int[]{3,2,4};
//        int results[] = new Solution1().twoSum(nums,6);
//        for (int result: results) {
//            System.out.println(result);
//        }
        List list = Arrays.asList(0,1,7,3,4,5,8);
        System.out.println(list.indexOf(7));;
        testSublist(list.subList(3,list.size()));
        int[] a = {0,1,2,3,4};


    }

    public static void testSublist(List<Integer> list){
        System.out.println(list);
    }
}