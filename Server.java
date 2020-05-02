import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
* This class implements a server which listens to the request on behalf 
* of a particular node in the chord ring.  Multi-threaded approach for 
* the server has been considered wherein requests are served in a child
* server, i.e, a different thread.  
* 
* @author Vijay Kumar
*/

public class Server implements Runnable {
    // Node on the behalf of which server will listen to the request 
    private Node node; 
    
    // ServerSocket at which server will listen to the request 
    private ServerSocket serverSocket; 
    
    // Flag that will determine when to stop the thread 
    private volatile boolean active;
    
    /**
    * Constructor to initialize the fields and start the serversocket.
    * 
    * @param node 
    *        Node on behalf of which this will listen to the request
    */
    Server(Node node) {
        this.node = node; 
        InetSocketAddress address = node.address; 
        int port = address.getPort(); 
        active = true; 
        
        try {
            serverSocket = new ServerSocket(port); 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
    * Method to direct when to stop the server. 
    */
    public void stop() {
        active = false; 
    }
    
    /**
    * The run method where the server listens to the request 
    * and creates new child server to serve the same. 
    */
    @Override
    public void run() {
        try {
            while (active) {
                Socket socket = serverSocket.accept(); 
                new Thread(new ChildServer(node, socket)).start();
            }
            serverSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.printf("Server has stopped functioning.\n");
    }
} 