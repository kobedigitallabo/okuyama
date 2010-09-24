package okuyama.imdst.manager;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.ServletHandler;

import okuyama.imdst.client.ImdstKeyValueClient;

public class OkuyamaManagerServer {

    static Server server = null;
    static boolean isStop = false;

    static String masterNodes = null;

    public static void main(String[] args) throws Exception {
        
        masterNodes = args[1];
        server = new Server();
        Connector connector = new SelectChannelConnector();
        connector.setPort(Integer.parseInt(args[0]));
        server.addConnector(connector);
        
        ServletHandler handler = new ServletHandler();
        
        // サーブレットクラスのマッピング
        handler.addServletWithMapping(okuyama.imdst.manager.servlet.MainServlet.class, "/okuyamamgr/*");
        
        server.addHandler(handler);
        
        server.start();
        server.join();
    }

    public static void stop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
    }

    public static ImdstKeyValueClient getClient() {
        ImdstKeyValueClient imdstKeyValueClient = null;
        String[] nodeRet = null;
        boolean setterFlg = false;
        try {
            imdstKeyValueClient = new ImdstKeyValueClient();
            imdstKeyValueClient.setConnectionInfos(masterNodes.split(","));
        } catch(Exception e) {
            e.printStackTrace();
        }
        return imdstKeyValueClient;
    }
}
