import java.util.concurrent.ThreadLocalRandom;

public class makeJSON {
    public static void main(String[] args) {

        String[] op = {"ADD","MUL","DIV","SUB","ADD","ADD","SUB","SUB","ADD","ADD","MUL"};
        System.out.print("{");   
        System.out.print("\"Map\":[");  
        System.out.print("{\"ADD\":\"2\"}");   
        int n = ThreadLocalRandom.current().nextInt(10, 100);
        for(int i=0;i<n;i++){
            int s = ThreadLocalRandom.current().nextInt(0, 11);
            int r = ThreadLocalRandom.current().nextInt(1, 20);
            System.out.print(",{\"" + op[s] + "\":\"" + r + "\"}");       
        } 
        System.out.print("]");   
        System.out.print(", \"CHANGEKEY\":\"NULL\", \"REDUCE\":[");
        System.out.print("{\"ADD\":\"NULL\"}");   
        n = ThreadLocalRandom.current().nextInt(10, 30);
        for(int i=0;i<n;i++){
            int s = ThreadLocalRandom.current().nextInt(0, 11);
            System.out.print(",{\"" + op[s] + "\":\"NULL\"}");       
        } 
        System.out.print("]");      
        System.out.print("}");      
    }

    
}