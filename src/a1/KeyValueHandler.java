package a1;

import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.lang.*;
import java.util.concurrent.Semaphore;

import org.apache.thrift.TException;
import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class KeyValueHandler implements KeyValueService.Iface {

	private HashMap<String, ByteBuffer> data;
	private HashMap<Integer,List<KeyValueService.Client>> connectionsPool;
	private HashMap<Integer, String> hosts;
    private HashMap<Integer, Integer> ports;
	private int total_servers, myNum;
	private Semaphore mutex;


	KeyValueHandler(int myNum,HashMap<Integer, String> hosts,HashMap<Integer, Integer> ports) {
		this.data = new HashMap<String, ByteBuffer>();
		this.connectionsPool = new HashMap<Integer, List<KeyValueService.Client>>();

		this.hosts = hosts;
		this.ports = ports;
		this.total_servers = hosts.size();
		this.myNum = myNum;
		this.mutex = new Semaphore(1);
	}

	public void setConnections(int node_number, List<KeyValueService.Client> clients){
		Integer key_node = new Integer(node_number);
		connectionsPool.put(key_node, clients);
	}

	public List<ByteBuffer> multiGet(List<String> keys){
		
		List<ByteBuffer> ret = new ArrayList<ByteBuffer>();

		try{
			List<ByteBuffer>[] holder = new ArrayList[total_servers];//holds result from each sn
			for (int x=0;x<holder.length;x++){
				holder[x] = new ArrayList<ByteBuffer>();
			}
			int[] position = new int [keys.size()];
			List<String>[] keyarray = new ArrayList[total_servers];
			for (int x=0;x<keyarray.length;x++){
				keyarray[x] = new ArrayList<String>();
			}
			int count = 0;
			while(!keys.isEmpty()){
				String key = keys.remove(0);
				int sn = key.hashCode()%total_servers;
				System.out.println("SN is "+sn);
				keyarray[sn].add(key);
				position[count] = sn;
				count++;
			}
			//grab connection, run on array of keys
			for (int i=0;i<keyarray.length;i++){
				if(!keyarray[i].isEmpty()&&i!=myNum){ // If key is in other node
					System.out.println("Condition 1 is " + keyarray[i].isEmpty());
					System.out.println("myNum is " + myNum);
					KeyValueService.Client client = getClient(i);

					System.out.println("Getting Client Number is "+i);

					holder[i] = client.multiGet(keyarray[i]); //arraylist of byte buffers for sn
					putClient(i,client);

				}
				else if(i==myNum&&!keyarray[i].isEmpty()){ // If key is in self
					while(!keyarray[i].isEmpty()){
						String key = keyarray[i].remove(0);
						System.out.println("Getting data for own key, key is "+key);

						if(data.containsKey(key)){
							holder[i].add(data.get(key));
						}
						else{
							ByteBuffer b = ByteBuffer.allocate(0);
							holder[i].add(b);
						}
					}
				}
			}
			//holder[i]remove(0) if position = i
			for (int k=0;k<position.length;k++){
				int tmp = position[k];//sn
				ret.add(holder[tmp].remove(0));
				System.out.println("concat part, adding from sn  "+tmp);

			}
		}
		catch(Exception x){
			x.printStackTrace();
			
		}
		return ret;
	}

	public List<ByteBuffer> multiPut(List<String> keys, List<ByteBuffer> values){
		//hash partition key
		//if %n==n, open client to that server node, and multiput it there
		//if %n is current server, just run it on the server
 		System.out.println("Start Multiput for SN "+ myNum);
		List<ByteBuffer> ret = new ArrayList<ByteBuffer>();

		try{
			List<ByteBuffer>[] holder = new ArrayList[total_servers];//holds result from each sn
			for (int x=0;x<holder.length;x++){
				holder[x] = new ArrayList<ByteBuffer>();
			}
			int[] position = new int [keys.size()];
			List<String>[] keyarray = new ArrayList[total_servers];
			for (int x=0;x<keyarray.length;x++){
				keyarray[x] = new ArrayList<String>();
			}
			List<ByteBuffer>[] valuearray = new ArrayList[total_servers];//holds result from each sn
			for (int x=0;x<valuearray.length;x++){
				valuearray[x] = new ArrayList<ByteBuffer>();
			}
			int count = 0;
			while(!keys.isEmpty()){
				String key = keys.remove(0);
				ByteBuffer value = values.remove(0);
				int sn = key.hashCode()%total_servers;
				System.out.println("SN is "+sn);
				keyarray[sn].add(key);
				valuearray[sn].add(value);
				position[count] = sn;
				count++;
			}
			//grab connection, run on array of keys
			for (int i=0;i<keyarray.length;i++){
				if(!keyarray[i].isEmpty()&&i!=myNum){ // If key is in other node
					System.out.println("Condition 1 is " + keyarray[i].isEmpty());
					System.out.println("myNum is " + myNum);
					KeyValueService.Client client = getClient(i);

					System.out.println("Getting Client Number is "+i);

					holder[i] = client.multiPut(keyarray[i],valuearray[i]); //arraylist of byte buffers for sn
					putClient(i,client);

				}
				else if(i==myNum&&!keyarray[i].isEmpty()){ // If key is in self
					while(!keyarray[i].isEmpty()){
						String key = keyarray[i].remove(0);
						ByteBuffer value = valuearray[i].remove(0);
						System.out.println("Getting data for own key, key is "+key);
						if(data.containsKey(key)){
							holder[i].add(data.get(key));
						}
						else{
							ByteBuffer b = ByteBuffer.allocate(0);
							holder[i].add(b);
						}
						data.put(key,value);

					}
				}
			}
			//holder[i]remove(0) if position = i
			for (int k=0;k<position.length;k++){
				int tmp = position[k];//sn
				ret.add(holder[tmp].remove(0));
				System.out.println("concat part, adding from sn  "+tmp);

			}
		}
		catch(Exception x){
			x.printStackTrace();
		}
		return ret;
		

		//check if lists are same size
		
		/*while(!keys.isEmpty()){
			String key = keys.remove(0);
			ByteBuffer value = values.remove(0);
			if (data.containsKey(key)){
				ByteBuffer tmp = data.get(key);
				ret.add(tmp);
			}

			data.put(key,value);

		}
		return ret;*/
	}

    public List<String> getGroupMembers()
    {
		List<String> ret = new ArrayList<String>();
		ret.add("a76chan");
		ret.add("i4leung");
		return ret;
    }

   private void putClient(int node_number, KeyValueService.Client client){
	    Integer key_node = new Integer(node_number);

   		try {
	    	mutex.acquire();
	    	List<KeyValueService.Client> clients = connectionsPool.get(key_node);
	    	clients.add(client);
	    	connectionsPool.put(key_node, clients);
	    	mutex.release();
    	}
    	catch(Exception x) {
    		x.printStackTrace();
    	}
    }

    private KeyValueService.Client getClient(int node_number){
    	if (node_number==myNum){
    		return null;
    	}
    	Integer key_node = new Integer(node_number);

    	try {
			mutex.acquire();
			List<KeyValueService.Client> clients = connectionsPool.get(key_node);
			KeyValueService.Client client = clients.remove(0);
			connectionsPool.put(key_node, clients);
			mutex.release();
			return client;
		}
		catch(Exception x) {
			x.printStackTrace();
		}

		System.out.println("existing client connecting to "+ node_number);
		return null;
    		

  	}
}

