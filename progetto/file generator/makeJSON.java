import java.util.concurrent.ThreadLocalRandom;

public class makeJSON {
    public static void main(String[] args) {

        String[] opMap = {"ADD","MUL","DIV","SUB","ADD","ADD","SUB","SUB","ADD","ADD","MUL","CHANGEKEY"};
        String[] opReduce = {"REDUCEADD","REDUCEMUL","REDUCESUB"};
        System.out.print("{");   
        System.out.print("\"Map\":[");  
        System.out.print("{\"ADD\":\"2\"}");   
        int n = ThreadLocalRandom.current().nextInt(10, 100);
        for(int i=0;i<n;i++){
            int s = ThreadLocalRandom.current().nextInt(0, 11);
            int r = ThreadLocalRandom.current().nextInt(1, 20);
            System.out.print(",{\"" + opMap[s] + "\":\"" + r + "\"}");       
        } 
        System.out.print("]");   
        System.out.print(",\"Reduce\":");
        int s = ThreadLocalRandom.current().nextInt(0, 3);
        System.out.print("{\"" + opReduce[s] + "\":\"NULL\"}");  
        System.out.print(",\"Chunks\":");
        int d = ThreadLocalRandom.current().nextInt(10, 30);
        System.out.print(d +"}");                    
    }

    
}