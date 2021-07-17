package HOT100;

import java.util.Stack;


class Solution53 {
    public int maxSubArray(int[] nums) {
//        int maxSum = -100000;
//        int len = nums.length;
//        int sum = 0;
//        for (int i = 1; i <= len; i++) {
//            for (int j = 0; j < len-i+1; j++) {
//                for (int k = j; k < j+i; k++) {
//                    sum += nums[k];
//                }
//                maxSum = sum>maxSum?sum:maxSum;
//                sum = 0;
//            }
//        }
//        return maxSum;
        int maxSum = nums[0], preSum = 0;
        for (int num: nums) {
            preSum = Math.max(num,preSum+num);
            maxSum = Math.max(preSum, maxSum);
        }
        return maxSum;
    }
}

public class problem53{
    public static void main(String[] args) {
        int sum[] = new int[]{5,4,-1,7,8};
        System.out.println(new Solution53().maxSubArray(sum));
    }
}