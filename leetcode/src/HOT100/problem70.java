package HOT100;


class Solution70 {
    public int climbStairs(int n) {
//        int p =0;
//        int q= 0;
//        int r= 1;
//        for (int i = 1; i <= n; i++) {
//            p = q;
//            q = r;
//            r = p + q;
//        }
//        return r;
        int sum[] = new int[n+1];

        if (n==0){
            return 1;
        }else if (n==1){
            return 1;
        }else {
            return climbStairs(n-1)+climbStairs(n-2);
        }
    }
}

public class problem70{
    public static void main(String[] args) {
        int n = 5;
        System.out.println(new Solution70().climbStairs(n));
    }
}