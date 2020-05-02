import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap; 
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

/**
* This class implements the Node of a chord ring. Implementation 
* consists of searching and updating the neighbors, fingers etc. 
* 
* @author Vijay Kumar
*/

public class Node {
    
    /**********************************************************************************************
    *                                                                                            *
    *                                       Fields                                               *
    *                                                                                            *
    **********************************************************************************************/
    
    
    // Public identifier of this node
    public int key; 
    
    // Server that will listen to the request on behalf of this node
    private Server server; 
    
    // InetSocketAddress of this node
    public InetSocketAddress address;
    
    // InetSocketAddress of the immediate predecessor of this node
    private InetSocketAddress predecessor; 
    
    // Contains the InetSocketAddress of the node at ith step in the finger table 
    public InetSocketAddress[] fingers;  
    
    // Contains the files along with their keys this node is responsible for 
    private Map<String, Integer> data; 
    
    // Maintains the successors pointers in the case of failures. 
    public Stabilize stabilize; 
    
    // Keeps track of some nearest successor of this node. 
    public NearestSuccessors nearestSuccessors; 
    
    // Maintains the finger table in the case of dynamic joins and failures.
    public FixFingers fixFingers; 
    
    
    /**********************************************************************************************
    *                                                                                            *
    *                                      Constructors                                          *
    *                                                                                            *
    **********************************************************************************************/
    
    
    /**
    * Constructor to be used when this is the first node to join in the chord ring. 
    * Initializes the predecessor and fingers to point to the current node itself. 
    * 
    * @param hostname
    *        Hostname of the current node
    * @param port
    *        port number of the current node
    */
    Node(String hostname, int port) {
        this.address = new InetSocketAddress(hostname, port);
        this.key = NodeUtility.hashValue(address);
        
        initializeFingerTable();
        moveKeys(100);
        
        server = new Server(this); 
        new Thread(server).start();
        
        startThreads();
    }
    
    /**
    * Constructor to be used when this node joins a chord ring which is already in existence. 
    * Initializes predecessor and finger table with the help of helper node provided. 
    * 
    * @param hostname
    *        Hostname of the current node
    * @param port 
    *        port number of the current node
    * @param helper
    *        InetSocketAddress of the node which will help in filling out the finger table
    */
    Node(String hostname, int port, InetSocketAddress helper) {
        this.address = new InetSocketAddress(hostname, port);
        this.key = NodeUtility.hashValue(address);
        
        initializeNeighbors(helper);
        
        /**
        * Starts the server just after initialization of neighbors and before the initialization
        * of finger table as the stabilize of predecessor would ask for its presence and no response
        * would be deemed as failure of the node. 
        */
        server = new Server(this); 
        new Thread(server).start();
        
        initializeFingerTable(helper);
        updateOthers();
        moveKeys(getSuccessor());
        startThreads();
    }
    
    
    /**********************************************************************************************
    *                                                                                            *
    *                                         Methods                                            *
    *                                                                                            *
    **********************************************************************************************/
    
    /**
    * Starts all threads of a node. 
    */
    private void startThreads() {
        stabilize = new Stabilize(this); 
        fixFingers = new FixFingers(this); 
        nearestSuccessors = new NearestSuccessors(this); 
        
        new Thread(stabilize).start();
        new Thread(fixFingers).start();
        new Thread(nearestSuccessors).start();
    }
    
    /**
    * Initializes neighbors of a node with the help of a helper 
    * node which is already there in the chord ring. 
    * 
    * @param helper
    *        InetSocketAddress of the helper node
    */
    private void initializeNeighbors(InetSocketAddress helper) {
        fingers = new InetSocketAddress[NodeUtility.NUMBER_OF_AVAILABLE_BITS];
        
        String response = NodeUtility.processRequest(helper, "FindSuccessor:" + this.key); 
        InetSocketAddress successor = NodeUtility.parseInetSocketAddress(response);
        Arrays.fill(fingers, successor);
        
        response = NodeUtility.processRequest(fingers[0], "YourPredecessor"); 
        this.predecessor = NodeUtility.parseInetSocketAddress(response);
        
        // Notifies successor about its presence
        NodeUtility.processRequest(successor, "Notify:" + this.address.toString());
    }
    
    /**
    * Initializes the finger table so that all the finger points to current node. 
    * To be used when this is the first node to join the chord ring. 
    */
    private void initializeFingerTable() {
        this.predecessor = this.address; 
        fingers = new InetSocketAddress[NodeUtility.NUMBER_OF_AVAILABLE_BITS];
        Arrays.fill(fingers, this.address);
    }
    
    /**
    * Initializes the finger table of this node with the help of node helper. 
    * To be used when this node joins a chord ring already in existence. 
    * 
    * @param helper fromIndex
    *        InetSocketAddress of the helper node
    */
    private void initializeFingerTable(InetSocketAddress helper) {
        
        for (int i = 1; i < NodeUtility.NUMBER_OF_AVAILABLE_BITS; i++) {
            InetSocketAddress lastFinger = fingers[i - 1]; 
            int lastFingerKey = NodeUtility.hashValue(lastFinger); 
            
            int lastFingerStart = (this.key + NodeUtility.getithStep(i - 1)) % NodeUtility.KEYSPACE; 
            int thisFingerStart = (this.key + NodeUtility.getithStep(i)) % NodeUtility.KEYSPACE;
            
            /**
            *                     Finger[i].start = this.key + pow(2, i - 1)
            * 
            *            ithFinger belongs to [Finger[i].start, Finger[i + 1].start)
            *      (i + 1)thFinger belongs to [Finger[i + 1].start, Finger[i + 2].start)
            * 
            * ithFinger of a node is essentially the successor of Finger[i].start. If there
            * is no node in the range where ithFinger should have been(3rd line of comment),
            * it looks for node beyond that. But that would also become the successor of 
            * Finger[i + 1].start. 
            * 
            * It means, if ithFinger doesn't belong to the specified range, then ith and (i + 1)th 
            * finger of node will have the same value. Thus, there is no need to run FindPredecessor
            * for (i + 1)th finger. 
            */
            if (!NodeUtility.belongs(lastFingerStart, true, thisFingerStart, false, lastFingerKey)) {
                fingers[i] = lastFinger; 
            } else {
                String response = NodeUtility.processRequest(helper, "FindSuccessor:" + thisFingerStart);
                fingers[i] = NodeUtility.parseInetSocketAddress(response); 
            }
        }
    }
    
    /**
    * Change the successor of this node. 
    * 
    * @param  potentialSuccessor
    *         InetSocketAddress of the new successor of this node
    * @return Acknowledgement message
    */
    public String changeSuccessor(InetSocketAddress potentialSuccessor) {
        synchronized(this) {
            fingers[0] = potentialSuccessor; 
        }
        return "Done"; 
    }
    
    /**
    * Change the predecessor of this node. 
    * 
    * @param  potentialPredecessor
    *         InetSocketAddress of the new predecessor of this node
    * @return Acknowledgement message
    */
    public String changePredecessor(InetSocketAddress potentialPredecessor) {
        synchronized(this) {
            this.predecessor = potentialPredecessor; 
        }
        return "Done"; 
    }
    
    /**
    * Checks whether the given node could be the predecessor of this node. 
    * If yes, make the possible changes. 
    * 
    * @param  potentialPredecessor
    *         InetSocketAddress of the node which could be the new predecessor
    * @return Acknowledgement message
    */
    public String notify(InetSocketAddress potentialPredecessor) {
        if (!isAlive(this.predecessor)) {
            return changePredecessor(potentialPredecessor);
        }
        
        int predecessorKey = NodeUtility.hashValue(this.predecessor); 
        int potentialPredecessorKey = NodeUtility.hashValue(potentialPredecessor); 
        
        if (NodeUtility.belongs(predecessorKey, false, this.key, false, potentialPredecessorKey)) {
            this.predecessor = potentialPredecessor; 
        } 
        return "Done"; 
    }
    
    /**
    * Checks whether the node with given address is alive or not. 
    * Used in cases where updation of the predecessor is required. 
    * 
    * @return true, if node with the given address is alive
    *         false, otherwise
    */
    private boolean isAlive(InetSocketAddress address) {
        String request = NodeUtility.processRequest(address, "Alive"); 
        return !(request == null); 
    }
    
    /**
    * Updates the ith finger entry in the finger table of this node if required. 
    * 
    * @param  i
    *         index of the finger which is to be checked for update
    * @param  finger
    *         InetSocketAddress of potential ith finger
    * @return Acknowledgement message
    */
    public String updateithFinger(int i, InetSocketAddress potentialFinger) {
        int currentFingerKey = NodeUtility.hashValue(fingers[i]); 
        int potentialFingerKey = NodeUtility.hashValue(potentialFinger); 
        
        if (NodeUtility.belongs(this.key, false, currentFingerKey, false, potentialFingerKey)) {
            fingers[i] = potentialFinger; 
            InetSocketAddress predecessor = this.predecessor; 
            return NodeUtility.processRequest(predecessor, 
            "UpdateithFinger:" + i + ":" + potentialFinger.toString()); 
        }
        return "Done"; 
    }
    
    /**
    * Updates the finger table of all the nodes which should have this node 
    * present in their finger table after this node has joined the chord ring.
    */
    public void updateOthers() {
        for (int i = 0; i < NodeUtility.NUMBER_OF_AVAILABLE_BITS; i++) {
            int requiredKey = this.key - NodeUtility.getithStep(i); 
            requiredKey += requiredKey < 0 ? NodeUtility.KEYSPACE : 0; 
            
            InetSocketAddress requiredAddressPredecessor = getPredecessor(requiredKey); 
            InetSocketAddress requiredAddressSuccessor = null; 
            
            if (requiredAddressPredecessor.equals(this.address)) {
                requiredAddressSuccessor = getSuccessor(); 
            } else {
                String response = NodeUtility.processRequest(requiredAddressPredecessor, "YourSuccessor"); 
                requiredAddressSuccessor = NodeUtility.parseInetSocketAddress(response); 
            }
            
            InetSocketAddress requiredAddress = (NodeUtility.isValidForithFingerUpdate(i, this.address, requiredAddressSuccessor)) 
                                                ? requiredAddressSuccessor 
                                                : requiredAddressPredecessor; 
            
            NodeUtility.processRequest(requiredAddress, "UpdateithFinger:" + i + ":" + this.address); 
        }
    }   
    
    /**
    * Generate 100 random files and transfer the responsiblity to this node. 
    * To be used when this is the first node in the chord ring. 
    * 
    * @param totalFiles
    *        Number of random files to be generated
    */
    private void moveKeys(int totalFiles) {
        data = new HashMap<>();
        String[] files = NodeUtility.generateRandomFiles(totalFiles);
        
        for (String filename : files) {
            int hashvalue = NodeUtility.hashValue(filename); 
            data.put(filename, hashvalue); 
        }
    }
    
    /**
    * Transfers keys from its successor for which this node is responsible now. 
    * Successor returns a string consisting of filenames separated by ':'.
    * 
    * @param successor
    *        InetSocketAddress of the successor of this node
    */
    public void moveKeys(InetSocketAddress successor) {
        data = new HashMap<>(); 
        String response = NodeUtility.processRequest(successor, "TransferKeys:" + this.key + 
        ":" + NodeUtility.hashValue(predecessor)); 
        
        String[] files = response.split(":"); 
        for (String filename : files) {
            int hashvalue = NodeUtility.hashValue(filename); 
            data.put(filename, hashvalue);
        }
    }
    
    /**
    * Finds the successor of this node.
    * 
    * @return InetSocketAddress of the successor of this node
    */
    public InetSocketAddress getSuccessor() {
        return fingers[0]; 
    }
    
    /**
    * Finds the successor of id. 
    * Successor of id is defined as the first node in the chord ring 
    * whose key is greater than or equal to id. 
    * 
    * @param  id
    *         key whose successor is sought
    * @return InetSocketAddress of the successor of id
    */
    public InetSocketAddress getSuccessor(int id) {
        InetSocketAddress predecessor = getPredecessor(id);
        String response = NodeUtility.processRequest(predecessor, "YourSuccessor"); 
        
        while (response == null) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
            
            predecessor = getPredecessor(id); 
            response = NodeUtility.processRequest(predecessor, "YourSuccessor"); 
        }
        return NodeUtility.parseInetSocketAddress(response); 
    }
    
    /**
    * Finds the predecessor of this node. 
    * 
    * @return InetSocketAddress of the predecessor of this node
    */
    public InetSocketAddress getPredecessor() {
        return this.predecessor; 
    }
    
    /**
    * Finds the node that will precede id.
    * Predecessor of id is defined as the first node in the chord ring 
    * whose key is less than id. 
    *
    * @param  id
    *         key for which the predecessor is sought
    * @return InetSocketAddress of the predecessor of id.
    */
    public InetSocketAddress getPredecessor(int id) {
        InetSocketAddress successor = getSuccessor(); 
        int successorKey = NodeUtility.hashValue(successor); 
        
        if (NodeUtility.belongs(this.key, false, successorKey, true, id)) {
            return this.address; 
        }
        
        InetSocketAddress closestPredecessor = getClosestPrecedingFinger(id);
        String response = NodeUtility.processRequest(closestPredecessor, "FindPredecessor:" + id); 
        
        while (response == null) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
            
            int closestPredecessorKey = NodeUtility.hashValue(closestPredecessor); 
            closestPredecessor = getClosestPrecedingFinger(closestPredecessorKey); 
            response = NodeUtility.processRequest(closestPredecessor, "FindPredecessor:" + id); 
        }
        return NodeUtility.parseInetSocketAddress(response); 
    }
    
    /**
    * Finds the farthest node in the finger table whose key precedes id. 
    * 
    * @param  id
    *         key whose preceding node is sought
    * @return InetSocketAddress of the closest preceding finger
    */
    public InetSocketAddress getClosestPrecedingFinger(int id) {
        
        for (int i = fingers.length - 1; i >= 0; i--) {
            InetSocketAddress fingerAddress = fingers[i]; 
            int fingerKey = NodeUtility.hashValue(fingerAddress);
            
            if (NodeUtility.belongs(this.key, false, id, false, fingerKey)) {
                return fingerAddress; 
            }
        }
        return this.address; 
    }
    
    /**
    * Transfers those filenames it contains which are supposed to be 
    * the responsibility of the new node with address predecessor. 
    * String format to be returned is filenames separated by colon. 
    
    *              Npre ---> Nnew ---> Nsuc
    * 
    * Node Npre and Nsuc were existing in the chord ring as neighbors of each 
    * other. Thus, Nsuc was responsible for all the keys which lies in the range
    * (Npre, Nsuc]. Buf after Nnew has joined the ring, the keys which lie in the 
    * range (Npre, Nnew] should be transferred to the node Nnew from Nsuc.
    * 
    * For Nsuc, Nnew is the firstPredecessor and Npre is the secondPredecessor. 
    * 
    * @param  firstPredecessorKey
    *         Key of the node which has asked to transfer files
    * @param  secondPredecessorKey
    *         Key of the predecessor of the immediate predecessor of this node
    * @return String representation of all filenames which are to be transferred
    */
    public String transferKeys(int firstPredecessorKey, int secondPredecessorKey) {
        StringBuilder result = new StringBuilder(); 
        Iterator<String> iterator = data.keySet().iterator();
        
        while (iterator.hasNext()) {
            String filename = iterator.next(); 
            int key = data.get(filename); 
            
            if (NodeUtility.belongs(secondPredecessorKey, false, firstPredecessorKey, true, key)) {
                result.append(filename + ":"); 
                iterator.remove(); 
            }
        }
        
        if (result.length() == 0) {
            return ""; 
        }
        
        // Deletes the last : used in this stringbuilder
        result = result.deleteCharAt(result.length()- 1); 
        return result.toString(); 
    }
    
    /**
    * Stops the functioning of this node. 
    */
    public void stop() {
        server.stop();
        stabilize.stop();
        fixFingers.stop();
        nearestSuccessors.stop();
    }
    
    /**
    * Prints the InetSocketAddress and ID of this node. 
    */
    public void printAddress() {
        System.out.printf("\nNode Information:\nID: %d\nHostname: %s\nPort: %d\n\n", 
        this.key, address.getHostName(), address.getPort()); 
    }
     
    /**
    * Prints the InetSocketAddress and ID of successor and predecessor
    * of this node. 
    */
    public void printNeighbors() {
        InetSocketAddress successor = getSuccessor(); 
        System.out.printf("\nSuccessor Information:\nID: %d\nHostname: %s\nPort: %d\n\n", 
        NodeUtility.hashValue(successor), successor.getHostName(), successor.getPort()); 
        
        InetSocketAddress predecessor = getPredecessor(); 
        System.out.printf("\nPredecessor Information:\nID: %d\nHostname: %s\nPort: %d\n\n", 
        NodeUtility.hashValue(predecessor), predecessor.getHostName(), predecessor.getPort()); 
    }
    
    /**
    * Prints the name of files contained by this node along with their keys. 
    */
    public void printContents() {
        Iterator<String> iterator = data.keySet().iterator(); 
        
        System.out.printf("\n%-16s%s\n\n", "Filename", "Key"); 
        while (iterator.hasNext()) {
            String filename = iterator.next(); 
            int key = NodeUtility.hashValue(filename); 
            System.out.printf("%-16s%d\n", filename, key); 
        }
        System.out.println(); 
    }
    
    /**
    * Prints the finger table of this node. 
    */
    public void printFingerTable() {
        System.out.printf("Finger Table\n\n"); 
        System.out.printf("%-8s%-10s%-14s%-8s%s\n\n", "S.NO.", "Start", "Hostname", "Key", "Port"); 
        
        for (int i = 0; i < NodeUtility.NUMBER_OF_AVAILABLE_BITS; i++) {
            int start = (this.key + NodeUtility.getithStep(i)) % NodeUtility.KEYSPACE; 
            InetSocketAddress finger = fingers[i]; 
            String hostname = finger.getHostName(); 
            int port = finger.getPort();
            int key = NodeUtility.hashValue(finger);
            System.out.printf("%-8d%-10d%-14s%-8d%d\n", i + 1, start, hostname, key, port); 
        }
        System.out.println(); 
    }
    
    
    
    /**
    * As of now, the protocol has been made to run in a single computer on different 
    * terminals wherein different terminal would act as different nodes.  As each
    * node would have their own port, it can be considered as two different nodes 
    * running from two different addresses making it similar to what happens in a 
    * chord ring. For the time being, user will provide the ID and the function will
    * use a utility method to find the port which will correspond to the given ID. 
    * Vice-versa could be implemented easily. Choices would be displayed on the terminal 
    * and function will generate the output on the console as per the user input. 
    */
    public static void main(String[] args) {
        Node node = null; 
        String hostname = "localhost"; 
        
        if (args.length == 1) {
            int ID = Integer.parseInt(args[0]); 
            node = new Node(hostname, NodeUtility.getPort(ID)); 
        } else {
            int ID = Integer.parseInt(args[0]); 
            int helperID = Integer.parseInt(args[1]); 
            int helperPort = NodeUtility.getPort(helperID); 
            InetSocketAddress helper = new InetSocketAddress(hostname, helperPort); 
            node = new Node(hostname, NodeUtility.getPort(ID), helper); 
        }
        
        Scanner in = new Scanner(System.in); 
        while (true) {
            System.out.printf("\n1: Print Address\n2: Print Neighbors\n3: Print Contents\n4: Print Successors\n" + 
            "5: Print Finger Table\n6: Search file\n7: Exit\n"); 
            System.out.printf("\nEnter choice: "); 
            int choice = in.nextInt(); 
            
            switch (choice) {
                case 1 : 
                    node.printAddress(); 
                    break;
                
                case 2: 
                    node.printNeighbors();
                    break;
                
                case 3: 
                    node.printContents(); 
                    break;
                
                case 4: 
                    node.nearestSuccessors.printSuccessors();
                    break;
                
                case 5: 
                    node.printFingerTable();
                    break; 
                
                case 6:
                    System.out.printf("Enter key to be searched: "); 
                    int fileID = in.nextInt(); 
                    InetSocketAddress successor = node.getSuccessor(fileID); 
                    int successorKey = NodeUtility.hashValue(successor); 
                    System.out.printf("ID of the successor node is : %d\n\n", successorKey); 
                    break;
                
                case 7:
                    node.stop();
                    break;
            }
            if (choice == 7) break;
        }
        in.close();
    }
}