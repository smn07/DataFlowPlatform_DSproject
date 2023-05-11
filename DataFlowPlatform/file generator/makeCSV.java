import java.util.concurrent.ThreadLocalRandom;

public class makeCSV {
    public static void main(String[] args) {
        int n = ThreadLocalRandom.current().nextInt(100, 1000);
        for(int i=0;i<n;i++){
            int s = ThreadLocalRandom.current().nextInt(0, 20);
            int r = ThreadLocalRandom.current().nextInt(0, 20);
            System.out.print(s + " , " + r + "\n");       
        } 
    }
}
