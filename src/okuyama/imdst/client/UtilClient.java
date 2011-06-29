
package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.zip.*;


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
            System.out.println("Command1. DataExport args1=dataexport args2=DataNode-IPAdress args3=DataNode-Port");
            System.out.println("Command2. TruncateData args1=truncatedata args2=MainMasterNode-IPAdress args3=MainMasterNode-Port args4=IsolationName or 'all'");
        }

        if (args[0].equals("dataexport") || args[0].equals("bkup")) {
            if (args.length != 3) {
                System.out.println("args[0]=Command, args[1]=serverip, args[2]=port");
                System.exit(1);
            }

            dataExport(args[1], Integer.parseInt(args[2]));
        }

        if (args[0].equals("truncatedata") || args[0].equals("truncatedata")) {
            if (args.length != 4) {
                System.out.println("args[0]=Command, args[1]=MainMasterNodeServerIp, args[2]=port, args[3]=IsolationPrefix or 'all'");
                System.exit(1);
            }

            truncateData(args[1], Integer.parseInt(args[2]), args[3]);
        }

        if (args[0].equals("fulldataexport")) {
            if (args.length != 3) {
                System.out.println("args[0]=Command, args[1]=serverip, args[2]=port");
                System.exit(1);
            }

            dataExport(args[1], Integer.parseInt(args[2]));
        }

    }


    public static  void dataExport(String serverip, int port) {
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

}
