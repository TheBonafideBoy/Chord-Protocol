import java.net.InetSocketAddress;
import java.util.Random;

/**
* This class implements FixFingers which takes care of updating
* the fingers upon dynamic joins and failures. 
* 
* @author Vijay Kumar
*/

public class FixFingers implements Runnable {
    // Node for which the thread will update the fingers
    private Node node; 
    
    // Random generator to find the random index to be updated
    private Random random; 
    
    /**
    * Boolean value to keep track of when to stop. Kept as volatile 
    * so that the value of active is always checked from the main 
    * memory instead of storing it in a cache. 
    */
    private volatile boolean active; 
    
    /**
    * Initializes the object.
    */
    FixFingers(Node node) {
        this.node = node;  
        this.active = true; 
        this.random = new Random(); 
    }
    
    /**
    * Stops the thread. 
    */ 
    public void stop() {
        this.active = false; 
    }
    
    /**
    * Selects a finger at random and updates its value. 
    */
    @Override
    public void run() {
        
        while (active) {
            int fingerIndex = random.nextInt(NodeUtility.NUMBER_OF_AVAILABLE_BITS - 1) + 1; 
            int ithStep = NodeUtility.getithStep(fingerIndex); 
            int fingerID = (node.key + ithStep) % NodeUtility.KEYSPACE;
            InetSocketAddress finger = node.getSuccessor(fingerID);  
            
            synchronized(node) {
                node.fingers[fingerIndex] = finger; 
            }
            
            try {
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        }
        
        System.out.printf("Thread FixFingers has stopped working.\n"); 
    }
}