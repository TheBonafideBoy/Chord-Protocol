import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter; 
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
* This is a final class which implements some utility methods
* and some constants to be used by the other classes like the
* calculation of hash value, generating random files, processing
* request on the behalf of a client etc.  Instantiation of the 
* class is neither permitted nor required, as all the methods 
* and constants are made static. 
* 
* @author Vijay Kumar
*/

public final class NodeUtility {
    // Total number of bits available for ID space
    public static final int NUMBER_OF_AVAILABLE_BITS = 5;
    
    // Total number of keys possible 
    public static final int KEYSPACE; 
    
    /**
    * Total number of successors whose InetSocketAddress is to be 
    * stored by NearestSuccessor object of the node. 
    */
    public static final int NUMBER_OF_NEAREST_SUCCESSORS = 2; 
    
    /**
    * Stores size of step for ith finger, 
    * i.e, for key = i, value = pow(2, i - 1)
    */
    private static final Map<Integer, Integer> STEPS; 
    
    // Stores the port values corresponding to a particular ID.
    private static final Map<Integer, Integer> PORTS; 
    
    // Initializes STEPS
    static {
        STEPS = initializeSteps();
        PORTS = initializePorts();
        KEYSPACE = 2 * getithStep(NUMBER_OF_AVAILABLE_BITS - 1); 
    }
    
    // Private constructor to ensure non-instantiability
    private NodeUtility() {}
    
    /**
    * Initialize STEPS. 
    */
    private static Map<Integer, Integer> initializeSteps() {
        Map<Integer, Integer> steps = new HashMap<>(); 
        steps.put(0, 1); 
        int value = 1; 
        
        for (int key = 1; key < NUMBER_OF_AVAILABLE_BITS; key++) {
            value *= 2; 
            steps.put(key, value);
        }
        return steps; 
    }
    
    /**
    * Initialize PORTS by taking data from a file Ports.csv
    */
    private static Map<Integer, Integer> initializePorts() {
        Map<Integer, Integer> ports = new HashMap<>(); 
        
        try {
            BufferedReader in = new BufferedReader(new FileReader("Ports.csv")); 
            String line = null; 
            in.readLine();
            in.readLine();
            
            while ((line = in.readLine()) != null) {
                String[] contents = line.split("\\s+");
                int key = Integer.parseInt(contents[0]); 
                int value = Integer.parseInt(contents[1]); 
                ports.put(key, value);
            }
            
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ports; 
    }
    
    /**
    * Finds the required port for given ID.
    * 
    * @param  ID
    *         ID of the node
    * @return Port associated with that ID.
    */
    public static int getPort(int ID) {
        return PORTS.get(ID); 
    }
    
    /**
    * Finds the size of ith step in finger table 
    *  
    * @param  i
    *         index in finger table whose step is to be found
    * @return size of ith step in finger table 
    */
    public static int getithStep(int i) {
        return STEPS.get(i); 
    } 
    
    /**
    * Returns the cryptographic hash of a String 
    * using SHA1 Algorithm.
    * Bit size of hash is bounded from above by 
    * NUMBER_OF_AVAILABLE_BITS.
    * 
    * @param  string
    *         String whose hash is to be determined
    * @return hash value of the String 
    */
    public static int hashValue(String string) {
        
        // Gets the 160 bit hash from SHA1
        byte[] bytes = getDigest(string);
        
        // Convert that 160 bits to a String 
        String s = getBinaryString(bytes); 
        
        // Do the XOR operation on all 32 possible 5 bit numbers
        int hash = 0; 
        for (int i = 0; i < 32; i++) {
            int beginIndex = i * 5; 
            String str = s.substring(beginIndex, beginIndex + 5); 
            int integer = Integer.parseInt(str, 2); 
            hash ^= integer; 
        }
        
        return hash; 
    }
    
    /**
    * Uses auxiliary method hashValue(String s) to calculate 
    * the hashvalue of given socket address.
    * 
    * @param  address
    *         Socket Address for which the hash is to be calculated
    * @return hash value of the Socket Address
    */
    public static int hashValue(InetSocketAddress address) {
        return hashValue(address.toString());  
    }
    
    /**
    * Calculates the 160 bit cryptographic hash of String using 
    * SHA1 algorithm. 
    * 
    * @param  s
    *         String whose hash is to be determined
    * @return array of bytes
    */
    private static byte[] getDigest(String s) {
        byte[] bytes = null; 
        
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA1"); 
            messageDigest.update(s.getBytes());
            bytes = messageDigest.digest(); 
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e.getMessage()); 
        }
        
        return bytes; 
    }
    
    /**
    * Takes an array of bytes. Convert a byte to its binary form
    * and append all of the bytes in array to form a binary string 
    * of length 160. 
    * A byte is extended to its 8 bit form even if it's actual 
    * length is not so. 
    * 
    * @param  bytes
    *         Array of bytes
    * @return binary representation of all bytes combined. 
    */
    private static String getBinaryString(byte[] bytes) {
        StringBuilder s = new StringBuilder(); 
        
        for (byte b : bytes) {
            s.append(formatByte(b)); 
        }
        return s.toString(); 
    }
    
    /**
    * Takes a byte and returns its binary representation in 8 bits.
    * 
    * @param  b
    *         byte to be converted
    * @return binary representation of the byte of length 8
    */
    private static String formatByte(byte b) {
        // To extend the length of positive byte above 8
        int i = 0x100 | b; 
        
        String s = Integer.toBinaryString(i); 
        
        return s.substring(s.length() - 8, s.length());
    }
    
    /**
    * Checks whether given ID belongs to the given range or not. 
    * 
    * Cases where border are included are dealt with by changing 
    * the border so that now border gets excluded. 
    * For example, 
    * >= 5 becomes > 5 - 1
    * <= 7 becomes < 7 + 1
    * 
    * Cases where right border is less than left border, is dealt
    * with by checking whether ID belongs to the remaining range 
    * or not. 
    * For example, 
    * ID belongs to (5, 1) is opposite of whether ID belongs to (1, 5) 
    * with border adjustments. 
    * 
    * @param  left
    *         left border
    * @param  leftInclusive
    *         whether left border is included or not 
    * @param  right
    *         right border
    * @param  rightInclusive
    *         whether right border is inluded or not 
    * @param  ID
    *         key to be searched 
    * @return true, if key lies in the range,
    *         false, otherwise
    */
    public static boolean belongs(int left, boolean leftInclusive,  
                                  int right, boolean rightInclusive, int ID) {
        
        if (left < right) {
            if (leftInclusive) left--; 
            if (rightInclusive) right++; 
            
            return left < ID && ID < right;
            
        } else if (left == right) {
            return leftInclusive || rightInclusive ? true : ID != left; 
            
        }
        return !belongs(right, !rightInclusive, left, !leftInclusive, ID);
    }
    
    /**
    * Generate given number of random names with some random extension 
    * as per a Random Generator.  
    * 
    * @param  totalFiles
    *         number of random files to be generated
    * @return array of strings consisting of name of random files
    */
    public static String[] generateRandomFiles(int totalFiles) {
        Random random = new Random(); 
        String[] files = new String[totalFiles]; 
        String[] extensions = new String[] {
            ".c", ".cpp", ".java", ".py", 
            ".txt", ".xml", ".csv", ".json" 
        };
        
        for (int i = 0; i < totalFiles; i++) {
            int length = random.nextInt(4) + 3; 
            String filename = getRandomString(length, random); 
            
            int extensionIndex = random.nextInt(extensions.length); 
            String extension = extensions[extensionIndex]; 
            
            files[i] = filename + extension; 
        }
        return files; 
    }
    
    /**
    * Generate a random string of given length using random characters.
    * 
    * @param  length
    *         length of random string
    * @param  random
    *         Random number generator 
    * @return string of desired length
    */
    private static String getRandomString(int length, Random random) {
        char[] c = new char[length]; 
        
        for (int i = 0; i < length; i++) {
            c[i] = (char) (random.nextInt(26) + 97); 
        }
        
        c[0] = Character.toUpperCase(c[0]); 
        return new String(c); 
    }
    
    /**
    * Communicates with server on the given address to get the request 
    * served and returns the response. 
    * 
    * @param  serverAddress 
    *         InetSocketAddress of the server
    * @param  request
    *         request that needs to be served
    * @return null, if there was any error in communication 
    *         response string from server, otherwise
    */
    public static String processRequest(InetSocketAddress serverAddress, String request) {
        try {
            Socket socket = new Socket(serverAddress.getAddress(), serverAddress.getPort());
            
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); 
            
            out.write(request + "\n");
            out.flush();
            
            String response = in.readLine(); 
            
            socket.close();
            
            return response; 
        } catch (Exception e) {
            return null; 
        }
    }
    
    /**
    * Parse the InetSocketAddress from the given presentation 
    * in the form of a string. 
    * 
    * For example, 
    * if s is amazon.in/52.95.116.115:80, then the result will have 
    * hostname = amazon.in
    * InetAddress = amazon.in/52.95.116.115
    * port = 80
    * 
    * @param  s
    *         String which is to be parsed
    * @return InetSocketAddress whose .toString() returns String s
    */
    public static InetSocketAddress parseInetSocketAddress(String s) {
        int indexOfSlash = s.indexOf("/"); 
        String hostname = s.substring(0, indexOfSlash); 
        
        int indexOfColon = s.lastIndexOf(":"); 
        String portAsString = s.substring(indexOfColon + 1, s.length()); 
        int port = Integer.parseInt(portAsString); 
        
        return new InetSocketAddress(hostname, port);
    }
    
    /**
    * Checks whether a finger is supposed to update its ith finger
    * after node joins in the chord ring.  
    * 
    * @param  i
    *         index of the finger that is to be updated
    * @param  recentlyJoinedNode
    *         InetSocketAddress of node which has recently joined the chord ring
    * @param  requestingNode
    *         InetSocketAddress of node who wants to update its ith finger
    * @return true, if requestingNode needs to update its ith finger, 
    *         false, otherwise
    */
    public static boolean isValidForithFingerUpdate(int i, InetSocketAddress recentlyJoinedNode, InetSocketAddress requestingNode) {
        int nodeKey = hashValue(recentlyJoinedNode); 
        int fingerKey = hashValue(requestingNode); 
        int requiredDistance = getithStep(i); 
        int actualDistance = -1; 
        
        if (nodeKey > fingerKey) {
            actualDistance = nodeKey - fingerKey; 
        } else {
            actualDistance = KEYSPACE - (fingerKey - nodeKey); 
        }
        return actualDistance == requiredDistance; 
    }
} 