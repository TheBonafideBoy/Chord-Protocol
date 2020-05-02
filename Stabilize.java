import java.net.InetSocketAddress;

/** 
* Implements a stabilizer for node which takes care of the updation of 
* successor of a node in the case of failures.  Hypothesis of the chord 
* protocol says that once the successor pointers are correct, lookup will
* eventually be correct. 
* 
* @author Vijay Kumar 
*/

public class Stabilize implements Runnable {
    /**
    * Node whose successor is to be maintained.
    */
    private Node node; 
    
    /**
    * Boolean value to keep track of when to stop. Kept as volatile 
    * so that the value of active is always checked from the main 
    * memory instead of storing it in a cache. 
    */
    private volatile boolean active; 
    
    /**
    * Initializes the object
    */
    Stabilize(Node node) {
        this.node = node; 
        this.active = true; 
    }
    
    /**
    * Stops the thread
    */
    public void stop() {
        this.active = false; 
    }
    
    @Override
    public void run() {
        while (active) {
            String response = NodeUtility.processRequest(node.getSuccessor(), "YourPredecessor"); 
            
            if (response == null) {
                InetSocketAddress successor = node.nearestSuccessors.nextSuccessor();
                node.changeSuccessor(successor); 
            } else {
                InetSocketAddress potentialSuccessor = NodeUtility.parseInetSocketAddress(response); 
                int currentSuccessorKey = NodeUtility.hashValue(node.getSuccessor()); 
                int potentialSuccessorKey = NodeUtility.hashValue(potentialSuccessor); 
                
                // Checks whether predecessor of its successor could be its new successor. 
                if (NodeUtility.belongs(node.key, false, currentSuccessorKey, false, potentialSuccessorKey)) {
                    node.changeSuccessor(potentialSuccessor); 
                    
                    /**
                    * Since the successor has been modified, the immediate successor is to be 
                    * changed in the successor list of object NearestSuccessors. Once the immediate
                    * successor has been made correct, as per the hypothesis, all successors 
                    * would eventually become correct. 
                    */
                    node.nearestSuccessors.successors[0] = potentialSuccessor; 
                }
            }
            
            // Notifies the successor about its presence. 
            NodeUtility.processRequest(node.getSuccessor(), "Notify:" + node.address.toString()); 
            
            try {
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        }
        
        System.out.printf("Thread Stabilize has stopped functioning.\n"); 
    }
} 