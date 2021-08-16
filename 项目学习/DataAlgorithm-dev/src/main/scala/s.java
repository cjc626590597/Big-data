import scala.math.Ordering;

public class s {
    public static void main(String[] args) {
        int a[] = {1,2,5};
        change(5, a);
    }

    public static int change(int amount, int[] coins) {
        int dup[] = new int[amount+1];
        dup[0] = 1;
        for(int coin:coins){
            for(int i=1; i<=amount; i++){
                if(coin <= i){
                    dup[i] = dup[i] + dup[i-coin];
                }
            }
        }
        return dup[amount];
    }
}
