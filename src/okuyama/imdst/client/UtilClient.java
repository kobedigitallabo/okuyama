
package okuyama.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.zip.*;


/**
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
        if (args.length < 3) {
            System.out.println("args[0]=Command, args[1]=serverip, args[2]=port");
        }

        if (args[0].equals("bkup")) {
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
            socket.connect(inetAddr, 5000);
            socket.setSoTimeout(10000);
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
}
