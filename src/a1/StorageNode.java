package a1;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.*;

public class StorageNode {

  private static class ClientCreator implements Runnable {
      private KeyValueHandler kvh;
      private HashMap<Integer, String> hosts;
      private HashMap<Integer, Integer> ports;
      private int myNum;

      ClientCreator(KeyValueHandler kvh, HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports, int myNum) {
            this.kvh = kvh;
            this.hosts = hosts;
            this.ports = ports;
            this.myNum = myNum;
      }

      public void run() {
            for (int i=0;i<hosts.size();i++){
                  if (i!=myNum){
                        List<KeyValueService.Client> clients = new ArrayList<KeyValueService.Client>();
                        for(int k=0;k<16;k++){
                              TSocket sock = new TSocket(hosts.get(i), ports.get(i));
                              TTransport transport = new TFramedTransport(sock);
                              TProtocol protocol = new TBinaryProtocol(transport);
                        
                              KeyValueService.Client client = new KeyValueService.Client(protocol);
                              clients.add(client);
                              int flag = 0;
                              while(flag == 0){
                                    try{
                                          flag = 1;
                                          transport.open();
                                    }
                                    catch(Exception x){
                                          flag = 0;
                                          continue;
                                    }
                              }
                        }     
                        kvh.setConnections(i, clients);
          
                        System.out.println("Connected to "+ i + "with 16 clients");
                  }
            }
      }
  }


  public static void main(String [] args) throws Exception {
      org.apache.log4j.BasicConfigurator.configure();

      if (args.length != 2) {
	  System.err.println("Usage: java a1.StorageNode config_file node_num");
	  System.exit(-1);
      }
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
      int myNum = Integer.parseInt(args[1]);
      System.out.println("My host and port: " + hosts.get(myNum) + ":" + ports.get(myNum));


      KeyValueHandler kvh = new KeyValueHandler(myNum, hosts, ports);
      ClientCreator cc = new ClientCreator(kvh, hosts, ports, myNum);
      Thread t = new Thread(cc);
      t.start();

      KeyValueService.Processor processor =new KeyValueService.Processor(kvh);

      // TServerSocket socket = new TServerSocket(ports.get(myNum));
      // TSimpleServer.Args sargs= new TSimpleServer.Args(socket);
      // sargs.protocolFactory(new TBinaryProtocol.Factory());
      // sargs.transportFactory(new TFramedTransport.Factory());
      // sargs.processorFactory(new TProcessorFactory(processor));
      // TServer server = new TSimpleServer(sargs);


      TNonblockingServerSocket socket = new TNonblockingServerSocket(ports.get(myNum));
      THsHaServer.Args sargs = new THsHaServer.Args(socket);
      sargs.protocolFactory(new TBinaryProtocol.Factory());
      sargs.transportFactory(new TFramedTransport.Factory());
      sargs.processorFactory(new TProcessorFactory(processor));
      sargs.maxWorkerThreads(Math.max(5, hosts.size()*16));
      TServer server = new THsHaServer(sargs);


      server.serve();
  }
}
