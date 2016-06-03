package a1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class Client {
  public static void main(String [] args) {
  	try {
      BufferedReader br = new BufferedReader(new FileReader(args[0]));
      HashMap<Integer, String> hosts = new HashMap<Integer, String>();
      HashMap<Integer, Integer> ports  = new HashMap<Integer, Integer>();
      String line;
      int i = 0;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(" ");
        hosts.put(i, parts[0]);
        ports.put(i, Integer.parseInt(parts[1]));
        i++;
      }

      System.out.println(hosts.get(0) + ports.get(0));
  		TSocket sock = new TSocket(hosts.get(0), ports.get(0));
  		TTransport transport = new TFramedTransport(sock);
  		TProtocol protocol = new TBinaryProtocol(transport);
      transport.open();
  		KeyValueService.Client client1 = new KeyValueService.Client(protocol);
      System.out.println("it acually works");
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      test1(client1);
      

      /*System.out.println(hosts.get(1) + ports.get(1));
      sock = new TSocket(hosts.get(1), ports.get(1));
      transport = new TFramedTransport(sock);
      protocol = new TBinaryProtocol(transport);
      KeyValueService.Client client2 = new KeyValueService.Client(protocol);
      transport.open();
      test1(client2);

      System.out.println(hosts.get(2) + ports.get(2));
      sock = new TSocket(hosts.get(2), ports.get(2));
      transport = new TFramedTransport(sock);
      protocol = new TBinaryProtocol(transport);
      KeyValueService.Client client3 = new KeyValueService.Client(protocol);
      transport.open();
      //test1(client3);
*/

  		System.out.println("It works");



  		transport.close();
  	}
  	catch (Exception x) {
      x.printStackTrace();
  	}
  }

  public static void test1(KeyValueService.Client client){
    try {
      List<String> listkeys = new ArrayList<String>();
        listkeys.add("key1");
        listkeys.add("key2");
        listkeys.add("key3");
        listkeys.add("key4");
        listkeys.add("key5");
       



        /*------------*/
        byte bytes[] = {1, 2, 3, 13, 5, 6, 20, 8};
        //ByteBuffer buf =byteArraytoByteBuffer(bytes);
        ByteBuffer buf =ByteBuffer.wrap(bytes);

        List<ByteBuffer> listbb = new ArrayList<ByteBuffer>();
        listbb.add(buf);
        listbb.add(buf);
        listbb.add(buf);
        listbb.add(buf);
        listbb.add(buf);

          System.out.println("it acually works");
         List<ByteBuffer> result1 = client.multiPut(listkeys, listbb);

         System.out.println("Result 1");
         while(!result1.isEmpty()){
          ByteBuffer value = result1.remove(0);
          byte byte_array[] = byteBuffertoByteArray(value);
          for(int i=0;i<byte_array.length;i++){
            System.out.print(byte_array[i]+ " ");
          }
          System.out.println("");
         }

          List<ByteBuffer> listbbresult = client.multiGet(listkeys);
        while (!listbbresult.isEmpty()){
          ByteBuffer valueget = listbbresult.remove(0);
          byte byte_array_get[] = byteBuffertoByteArray(valueget);
          for(int i=0;i<byte_array_get.length;i++){
            System.out.print(byte_array_get[i]+ " ");
          }
          System.out.println("");
        }
     }
     catch (Exception x){
      x.printStackTrace();
     }
  }

  public static void test2(KeyValueService.Client client){
    try{

    }
    catch (Exception x){
      x.printStackTrace();
    }
  }

  public static byte[] byteBuffertoByteArray(ByteBuffer bb){

      byte byte_array[] = new byte[bb.remaining()];


      bb.get(byte_array);
      return byte_array;
  }
  public static ByteBuffer byteArraytoByteBuffer(byte[] b){
      ByteBuffer buf = ByteBuffer.wrap(b);
      return buf;
  }
}

