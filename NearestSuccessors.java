import java.net.InetSocketAddress;
import java.util.Random;

/**
* This class implements the details regarding storage of some 
* nearest successors of a particular node in the chord ring.
* Relevance is in successfully updating the immediate successor 
* of a node, in case a failure occurs. 
* 
* @author Vijay Kumar
*/

public class NearestSuccessors implements Runnable {
    // Node whose successors is to be contained.
    private Node node; 
    
    // Random generator to get the random index whose successor is to be updated
    private Random random;  
    
    /**
    * Boolean value to keep track of when to stop. Kept as volatile 
    * so that the value of active is always checked from the main 
    * memory instead of storing it in a cache. 
    */
    private volatile boolean active; 
    
    /**
    * Contains the list of r nearest successors of the node. Value of r 
    * has been mentioned in NodeUtility class. 
    */
    public InetSocketAddress[] successors; 
    
    /**
    * Initializes the object. 
    */
    NearestSuccessors(Node node) {
        this.node = node; 
        this.active = true; 
        this.random = new Random(); 
        this.successors = new InetSocketAddress[NodeUtility.NUMBER_OF_NEAREST_SUCCESSORS + 1];
        initialize();
    }
    
    /**
    * Initializes the successors list. 
    */
    private void initialize() {
        successors[0] = node.getSuccessor();
        
        for (int i = 1; i < NodeUtility.NUMBER_OF_NEAREST_SUCCESSORS; i++) {
            InetSocketAddress currentSuccessor = successors[i - 1]; 
            String response = NodeUtility.processRequest(currentSuccessor, "YourSuccessor"); 
            InetSocketAddress nextSuccessor = NodeUtility.parseInetSocketAddress(response); 
            successors[i] = nextSuccessor; 
        }
    }
    
    /**
    * When a successor at a particular index fails, it shifts 
    * all index to the right of it, to one step left, from left 
    * to right. 
    * 
    *       N0 -> N1 -> N2 -> N3 -> N4 -> N5
    * 
    * Consider the above example. If N3 fails, the nearest successor list 
    * of N0 should change to N1 -> N2 -> N4 -> N5, i.e, to shift, all nodes 
    * to the right of failed node, to one step left. 
    * 
    * @param invalidSuccessorIndex 
    *        Index of the successor node which has failed
    */
    public void shiftSuccessors(int invalidSuccessorIndex) {
        synchronized(successors) {
            for (int i = invalidSuccessorIndex; i < NodeUtility.NUMBER_OF_NEAREST_SUCCESSORS; i++) {
                successors[i] = successors[i + 1]; 
            }
        }
    }
    
    /**
    * Called by Stabilize when the immediate successor of the node 
    * has failed. 
    * 
    * @return InetSocketAddress of the new successor of this node
    */
    public InetSocketAddress nextSuccessor() {
        shiftSuccessors(0);
        return successors[0]; 
    }
    
    /**
    * Stops this node. 
    */
    public void stop() {
        this.active = false; 
    }
    
    /**
    * Prints all the successors of this node. 
    */
    public void printSuccessors() {
        System.out.printf("Nearest Successors\n\n"); 
        System.out.printf("%-8s%-8s\n\n", "S.No.", "Succesor Key"); 
        for (int i = 0; i <= NodeUtility.NUMBER_OF_NEAREST_SUCCESSORS; i++) {
            InetSocketAddress successor = successors[i]; 
            int successorKey = NodeUtility.hashValue(successor); 
            System.out.printf("%-8d%-8d\n",i, successorKey); 
        }
        System.out.println(); 
    }
    
    /**
    * Hypothesis behind the implementation of nearest successor is that if the 
    * immediate successor in the successor list is right, then the whole successor 
    * list would be right eventually. 
    */
    @Override
    public void run() {
        while (active) {
            int index = random.nextInt(NodeUtility.NUMBER_OF_NEAREST_SUCCESSORS); 
            InetSocketAddress successorUnderScrutiny = successors[index]; 
            String response = NodeUtility.processRequest(successorUnderScrutiny, "YourSuccessor"); 
            
            if (response != null) {
                InetSocketAddress updatedNextSuccessor = NodeUtility.parseInetSocketAddress(response); 
                synchronized(successors) {
                    successors[index + 1] = updatedNextSuccessor; 
                }
            } else if (index != 0) {
                shiftSuccessors(index);
            }
            
            /**
            * If the immediate successor has failed, then nothing is done as updation 
            * of immediate successor is the job of stabilize. In such a case, within 
            * no time, stabilize would call the method nextSuccessor of this object 
            * and updates the first successor. As per the hypothesis, list would be
            * true eventually. 
            */
            
            try {
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        }
        
        System.out.printf("Thread Nearest Successors has stopped functioning.\n"); 
    }
}