package org.imdst.manager.servlet;

import java.io.IOException;
import java.util.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.imdst.manager.OkuyamaManagerServer;
import org.imdst.client.ImdstKeyValueClient;
import org.imdst.util.ImdstDefine;


public class MainServlet extends HttpServlet {
    private StringBuffer pageBuf = null;

    public MainServlet() {
    }

    public void init(ServletConfig config) throws ServletException
    {
        super.init(config);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        doGet(request, response);
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        response.setContentType("text/html");
        ServletOutputStream out = response.getOutputStream();
        HashMap data = new HashMap();

        ImdstKeyValueClient imdstKeyValueClient = null;
        String[] nodeRet = null;

        try {
            imdstKeyValueClient = OkuyamaManagerServer.getClient();

            imdstKeyValueClient.autoConnect();
            ArrayList settingList = new ArrayList();
            HashMap setting = null;
            String[] clientRet = null;

            String[] settingParams = new String[10];
            settingParams[0] = ImdstDefine.Prop_KeyMapNodesInfo;
            settingParams[1] = ImdstDefine.Prop_SubKeyMapNodesInfo;
            settingParams[2] = ImdstDefine.Prop_KeyMapNodesRule;
            settingParams[3] = ImdstDefine.Prop_LoadBalanceMode;
            settingParams[4] = ImdstDefine.Prop_TransactionMode;
            settingParams[5] = ImdstDefine.Prop_TransactionManagerInfo;
            settingParams[6] = ImdstDefine.Prop_MainMasterNodeMode;
            settingParams[7] = ImdstDefine.Prop_SlaveMasterNodes;
            settingParams[8] = ImdstDefine.Prop_MainMasterNodeInfo;
            settingParams[9] = ImdstDefine.Prop_AllMasterNodeInfo;

            for (int idx = 0; idx < settingParams.length; idx++) {

                clientRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + settingParams[idx]);
                setting = new HashMap();
                setting.put("param", settingParams[idx]);
                setting.put("value", "&nbsp;");
                if (clientRet[0].equals("true")) {
                    setting.put("value", clientRet[1]);
                }
                settingList.add(setting);
            }
            out.println(initPage(settingList));
            out.flush();

        } catch(Exception e) {
            out.println("<html>");
            out.println("<h1>Distributed Key Value Store Okuyama Manager Console</h1>");
            out.println("<h2>Error <h2>");
            out.println("</html>");
            out.flush();

            e.printStackTrace();
        }
    }
    

    private String initPage(ArrayList settingList) {
        StringBuffer pageBuf = new StringBuffer();
        
        pageBuf.append("<html>");
        pageBuf.append("  <head>");
        pageBuf.append("    <meta http-equiv='Content-Type' content='text/html; charset=UTF-8' />");
        pageBuf.append("    <title>okuyama manager console</title>");
        pageBuf.append("  </head>");
        pageBuf.append("  <body>");
        pageBuf.append("    <table width=1024px align=center boder=0>");
        pageBuf.append("    <tr><td align=center>");
        pageBuf.append("    <h1>Distributed Key Value Store Okuyama Manager Console</h1>");
        pageBuf.append("    <h2>Manager Display</h2>");
        pageBuf.append("    <br />");
        pageBuf.append("    <hr />");
        pageBuf.append("    <table style='width:500px;' border=1>");
        pageBuf.append("      <tr>");
        pageBuf.append("        <td colspan=2 align=center bgcolor='Silver'>");
        pageBuf.append("          Okuyama Set up information");
        pageBuf.append("        </td>");
        pageBuf.append("      </tr>");

        for (int i = 0; i < settingList.size(); i++) {
            HashMap setting = (HashMap)settingList.get(i);
            pageBuf.append("<tr>");
            pageBuf.append("  <td>");
            pageBuf.append("    <p>" + (String)setting.get("param") + "</p>");
            pageBuf.append("  </td>");
            pageBuf.append("  <td>");
            pageBuf.append((String)setting.get("value"));
            pageBuf.append("  </td>");
            pageBuf.append("</tr>");
        }
        pageBuf.append("    </td></tr>");
        pageBuf.append("    </table>");
        pageBuf.append("    </table>");
        pageBuf.append("  </body>");
        pageBuf.append("</html>");
        return pageBuf.toString();
    }
}

