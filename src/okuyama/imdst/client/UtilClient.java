package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.zip.*;

import okuyama.imdst.util.*;

/**
 * okuyama用のUtilityクライアント.<br>
 * 機能
 * 1.データbackup
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class UtilClient {

    /**
     * コンストラクタ
     *
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("args[0]=Command");
            System.out.println("Command1. DataExport args1=bkup args2=DataNode-IPAdress args3=DataNode-Port args4=ClientArrivalCheckTime(Option) args5=DataReadWaitTime(Option - milli)");
            System.out.println("Command2. TruncateData args1=truncatedata args2=MainMasterNode-IPAdress args3=MainMasterNode-Port args4=IsolationName or 'all'");
            System.out.println("Command3. MasterNodeConfigCheck args1=masterconfig args2=MainMasterNode-IPAdress args3=MainMasterNode-Port");
            System.out.println("Command4. DataNode is added args1=adddatanode args2=MasterNode-IPAdress:PortNo args3=DataNodeIPAddress:PortNo args4=Slave1-DataNodeIpAddress:PortNo args5=Slave2-DataNodeIpAddress:PortNo");
            System.exit(1);
        }

        if (args[0].trim().equals("dataexport") || args[0].trim().equals("bkup")) {
            if (args.length < 3) {
                System.out.println("Argument Error! args[0]=dataexport or bkup, args[1]=serverip, args[2]=port");
                System.exit(1);
            }

            if (args.length > 4) {
                dataExport(args[1], Integer.parseInt(args[2]), args[3] , args[4]); 
            } else if (args.length > 3) {
                dataExport(args[1], Integer.parseInt(args[2]), args[3] , "0"); 
            } else {
                dataExport(args[1], Integer.parseInt(args[2]), "1", "0"); 
            }
        }

        if (args[0].trim().equals("truncatedata")) {
            if (args.length != 4) {
                System.out.println("Argument Error! args[0]=truncatedata, args[1]=MainMasterNodeServerIp, args[2]=port, args[3]=IsolationPrefix or 'all'");
                System.exit(1);
            }

            truncateData(args[1], Integer.parseInt(args[2]), args[3]);
        }


        if (args[0].trim().equals("masterconfig")) {
            if (args.length != 3) {
                System.out.println("Argument Error! args[0]=masterconfig, args[1]=MainMasterNodeServerIp, args[2]=port");
                System.exit(1);
            }

            masterNodeConfigCheck(args[1], Integer.parseInt(args[2]));
        }


        if (args[0].equals("fulldataexport")) {
            if (args.length != 3) {
                System.out.println("Argument Error! args[0]=Command, args[1]=serverip, args[2]=port");
                System.exit(1);
            }

            if (args.length > 4) {
                dataExport(args[1], Integer.parseInt(args[2]), args[3] , args[4]); 
            } else if (args.length > 3) {
                dataExport(args[1], Integer.parseInt(args[2]), args[3] , "0"); 
            } else {
                dataExport(args[1], Integer.parseInt(args[2]), "1", "0"); 
            }
        }


        if (args[0].equals("adddatanode")) {
            if (args.length < 3) {
                System.out.println("Argument Error! args[0]=Command, args[1]=MasterNodeIp:Port, args[2]=DataNodeIP:Port");
                System.exit(1);
            }

            List addNodeList = new ArrayList(3);
            addNodeList.add(args[2]);
            
            if (args.length > 3) {
                addNodeList.add(args[3]);
            }
            
            if (args.length > 4) {
                addNodeList.add(args[4]);
            }
            addDataNode(args[1], addNodeList);
        }
    }


    public static  void dataExport(String serverip, int port, String checkWaitTime, String readWaitTime) {
        Socket socket = null;
        PrintWriter pw = null;
        BufferedReader br = null;


        try {
            socket = new Socket();
            InetSocketAddress inetAddr = new InetSocketAddress(serverip, port);
            socket.connect(inetAddr, 10000);
            socket.setSoTimeout(60000);
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")));
            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

            pw.println("101");
            pw.flush();
            pw.println(checkWaitTime + "," + readWaitTime);
            pw.flush();


            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.equals("-1")) break;

                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null) socket.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }


    public static  void truncateData(String serverip, int port, String isolationPrefix) {
        Socket socket = null;
        PrintWriter pw = null;
        BufferedReader br = null;

        try {
            socket = new Socket();
            InetSocketAddress inetAddr = new InetSocketAddress(serverip, port);
            socket.connect(inetAddr, 10000);
            socket.setSoTimeout(13600000);
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")));
            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

            pw.println("61," + isolationPrefix);
            pw.flush();
            System.out.println("Truncate Execute");
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null) socket.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }


    public static void masterNodeConfigCheck (String serverip, int port) {
        Socket socket = null;
        PrintWriter pw = null;
        BufferedReader br = null;

        try {
            socket = new Socket();
            InetSocketAddress inetAddr = new InetSocketAddress(serverip, port);
            socket.connect(inetAddr, 10000);
            socket.setSoTimeout(10000);
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8")));
            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));

            pw.println("998");
            pw.flush();
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null) socket.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    

    public static void addDataNode(String masterNodeIpPort, List addNodeList) {
        OkuyamaClient client = null;

        try {
            client = new OkuyamaClient();
            client.setConnectionInfos(masterNodeIpPort.split(","));
            client.autoConnect();

            String[] algorithmRet = client.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_DistributionAlgorithm);

            if (algorithmRet[0].equals("true")) {

                if (algorithmRet[1].equals(ImdstDefine.dispatchModeConsistentHash)) {

                    int replicaType = 0;
                    String[] dataNodeRet = client.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo);
                    String[] slaveDataNodeRet = client.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo);
                    String[] thirdDataNodeRet = client.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_ThirdKeyMapNodesInfo);

                    if (dataNodeRet[0].equals("true")) replicaType++;
                    if (slaveDataNodeRet[0].equals("true")) replicaType++;
                    if (thirdDataNodeRet[0].equals("true")) replicaType++;
                    
                    if (replicaType != addNodeList.size()) {
                        System.out.println("[Error] - The number of the replicas of DataNode to add differs from the number of replicas of DataNode set up now");
                    } else {

                        StringBuilder addAllNodeInfos = new StringBuilder();
                        boolean setUpRet = false;
                        if (replicaType > 0) {
                            setUpRet = client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo, dataNodeRet[1] + "," + (String)addNodeList.get(0));
                            if (!setUpRet) client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_KeyMapNodesInfo, dataNodeRet[1]);
                            addAllNodeInfos.append((String)addNodeList.get(0));
                        }
                        if (!setUpRet) {
                            System.out.println("[Error] - When registering the information on DataNode, the error occurred. AddDataNodeName[" +  (String)addNodeList.get(0) + "]");
                            return;
                        }
                     
                        if (replicaType > 1) {
                            setUpRet = client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo, slaveDataNodeRet[1] + "," + (String)addNodeList.get(1));
                            if (!setUpRet) client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_SubKeyMapNodesInfo, slaveDataNodeRet[1]);
                            addAllNodeInfos.append(",");
                            addAllNodeInfos.append((String)addNodeList.get(1));
                        }
                        if (!setUpRet) {
                            System.out.println("[Error] - When registering the information on DataNode, the error occurred. AddDataNodeName[" +  (String)addNodeList.get(1) + "]");
                            return;
                        }

                        if (replicaType > 2) {
                            client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_ThirdKeyMapNodesInfo, thirdDataNodeRet[1] + "," + (String)addNodeList.get(2));
                            if (!setUpRet) client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_ThirdKeyMapNodesInfo, thirdDataNodeRet[1]);

                            addAllNodeInfos.append(",");
                            addAllNodeInfos.append((String)addNodeList.get(2));
                        }
                        if (!setUpRet) {
                            System.out.println("[Error] - When registering the information on DataNode, the error occurred. AddDataNodeName[" +  (String)addNodeList.get(2) + "]");
                            return;
                        }
                        
                        setUpRet = client.setValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.addNode4ConsistentHashMode, addAllNodeInfos.toString());
                        if (!setUpRet) client.removeValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.addNode4ConsistentHashMode);
                        
                        if (setUpRet) {
                            System.out.println("[Success] - The additional application of DataNode was completed");
                        }  else {
                            System.out.println("[Error] - The additional application of DataNode went wrong");
                        }
                    }
                } else {
                    System.out.println("[Error] - DataNode can be added only when 'DistributionAlgorithm' is 'consistenthash'");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (client != null) client.close();
            } catch (Exception e2) {
            }
        }
    }

}
