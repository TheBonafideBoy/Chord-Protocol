import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
* This class implements a childserver which actually serves the request that was 
* directed towards this server from the main server responsible for a particular 
* node.  One childserver serves to only one request. 
* 
* @author Vijay Kumar
*/

public class ChildServer implements Runnable {
    // Node on behalf of which server will serve the request
    private Node node; 
    
    // Socket through which server will communicate 
    private Socket socket; 
    
    /**
    * Initializes the necessary fields 
    * 
    * @param node  
    *        Node on the behalf of which server will serve the request
    * @param socket
    *        Socket through which server will communicate 
    */
    public ChildServer(Node node, Socket socket) {
        this.node = node; 
        this.socket = socket; 
    }
    
    /**
    * Processes the request with the help of node and utility methods. 
    * Format of the request string is 
    * Command : Variables 
    * Command decides the action that needs to be taken. 
    * Parameters contain some data on which the action is to be taken as 
    * as per the command.  Parameters may remain empty, wherever required.
    *
    * @param  request
    *         Request to be served
    * @return Appropriate response 
    */
    private String process(String request) {
        
        String[] commands = request.split(":"); 
        String command = commands[0]; 
        
        switch (command) {
            case "YourSuccessor": 
                return node.getSuccessor().toString(); 
            
            case "YourPredecessor": 
                return node.getPredecessor().toString(); 
            
            case "FindSuccessor": 
                int id = Integer.parseInt(commands[1]); 
                return node.getSuccessor(id).toString(); 
            
            case "FindPredecessor":
                id = Integer.parseInt(commands[1]); 
                return node.getPredecessor(id).toString(); 
            
            case "ChangePredecessor": 
                InetSocketAddress potentialPredecessor = NodeUtility.parseInetSocketAddress
                                                         (commands[1] + ":" + commands[2]); 
                return node.changePredecessor(potentialPredecessor); 
            
            case "ChangeSuccessor":
                InetSocketAddress potentialSuccessor = NodeUtility.parseInetSocketAddress
                                                       (commands[1] + ":" + commands[2]); 
                return node.changeSuccessor(potentialSuccessor);
            
            case "UpdateithFinger":
                int i = Integer.parseInt(commands[1]);
                InetSocketAddress potentialFinger = NodeUtility.parseInetSocketAddress
                                                    (commands[2] + ":" + commands[3]); 
                return node.updateithFinger(i, potentialFinger);
            
            case "TransferKeys":
                int firstPredecessorKey = Integer.parseInt(commands[1]); 
                int secondPredecessorKey = Integer.parseInt(commands[2]); 
                return node.transferKeys(firstPredecessorKey, secondPredecessorKey);
            
            case "Notify": 
                potentialPredecessor = NodeUtility.parseInetSocketAddress
                                       (commands[1] + ":" + commands[2]); 
                return node.notify(potentialPredecessor);
            
            case "Alive":
                return "Yes Baby"; 
            
            default:
                return "Done"; 
        }
    }
    
    /**
    * The run method where the actuall communication takes place. 
    */
    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); 
            
            String request = in.readLine(); 
            
            String response = process(request); 
            
            out.write(response + "\n");
            out.flush();
            
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
} 