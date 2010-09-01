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

    private boolean subNode = false;
    private boolean thirdNode = false;

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
        String msg = "&nbsp;";
        String dispatchAlgorithm = "mod";
        String addConsistentNodeStr = null;
        String addConsistentMainNodeStr = null;
        String addConsistentSubNodeStr = null;
        String addConsistentThirdNodeStr = null;

        try {

            ArrayList settingList = new ArrayList();
            ArrayList dataNodeList = new ArrayList();
            ArrayList slaveDataNodeList = new ArrayList();
            ArrayList thirdDataNodeList = new ArrayList();
            HashMap setting = null;
            String[] clientRet = null;
            String dataNodeStr = null;
            String slaveDataNodeStr = null;
            String thirdDataNodeStr = null;

            String[] settingParams = new String[10];
            settingParams[0] = ImdstDefine.Prop_KeyMapNodesInfo;
            settingParams[1] = ImdstDefine.Prop_SubKeyMapNodesInfo;
            settingParams[2] = ImdstDefine.Prop_ThirdKeyMapNodesInfo;
            settingParams[3] = ImdstDefine.Prop_KeyMapNodesRule;
            settingParams[4] = ImdstDefine.Prop_DistributionAlgorithm;
            settingParams[5] = ImdstDefine.Prop_LoadBalanceMode;
            settingParams[6] = ImdstDefine.Prop_TransactionMode;
            settingParams[7] = ImdstDefine.Prop_TransactionManagerInfo;
            settingParams[8] = ImdstDefine.Prop_MainMasterNodeInfo;
            settingParams[9] = ImdstDefine.Prop_AllMasterNodeInfo;


            // MasterNodeに接続
            imdstKeyValueClient = OkuyamaManagerServer.getClient();
            imdstKeyValueClient.nextConnect();

            // 振り分けアルゴリズムを取得
            clientRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + ImdstDefine.Prop_DistributionAlgorithm);
            if (clientRet[0].equals("true")) {
                dispatchAlgorithm = clientRet[1];
            }
            clientRet = null;



            // 変更が実行されている場合は更新を実行
            if (request.getParameter("execmethod") != null && 
                    ((String)request.getParameter("execmethod")).equals("modparam") &&
                        (String)request.getParameter("updateparam") != null) {


                // 複数のParameterを一度に更新する場合はupdateparamに"#"区切りでパラメータ名が渡される想定
                String[] updateParams = ((String)request.getParameter("updateparam")).split("#");
                for (int idx = 0; idx < updateParams.length; idx++) {

                    // Rule値は設定されたサーバ台数で自動的に作成する
                    if (updateParams[idx].equals(ImdstDefine.Prop_KeyMapNodesRule)) {

                        String[] nextKeyNodes = ((String)request.getParameter(ImdstDefine.Prop_KeyMapNodesInfo)).trim().split(",");
                        String[] nowRules = ((String)request.getParameter(ImdstDefine.Prop_KeyMapNodesRule)).trim().split(",");
                        String nextRulesStr = (String)request.getParameter(ImdstDefine.Prop_KeyMapNodesRule);

                        if (nextKeyNodes.length != Integer.parseInt(nowRules[0])) {
                            nextRulesStr = nextKeyNodes.length + "," + nextRulesStr;
                        }
                        if(!this.execModParam(imdstKeyValueClient, updateParams[idx], nextRulesStr)) 
                                                    throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");

                    } else if (updateParams[idx].equals("addMainDataNode")) {

                        if ((String)request.getParameter("addMainDataNode") != null && !(((String)request.getParameter("addMainDataNode")).trim()).equals("")) 
                            addConsistentMainNodeStr = (String)request.getParameter("addMainDataNode");
                    } else if (updateParams[idx].equals("addSubDataNode")) {

                        if ((String)request.getParameter("addSubDataNode") != null && !(((String)request.getParameter("addSubDataNode")).trim()).equals("")) 
                            addConsistentSubNodeStr = (String)request.getParameter("addSubDataNode");
                    } else if (updateParams[idx].equals("addThirdDataNode")) {

                        if ((String)request.getParameter("addThirdDataNode") != null && !(((String)request.getParameter("addThirdDataNode")).trim()).equals("")) 
                            addConsistentThirdNodeStr = (String)request.getParameter("addThirdDataNode");
                    } else {

                        if(!this.execModParam(imdstKeyValueClient, 
                                                updateParams[idx], 
                                                (String)request.getParameter(updateParams[idx]))) 
                                                    throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                    }
                    msg = "Update Parameter Success";
                }


                if (request.getParameter(ImdstDefine.Prop_KeyMapNodesInfo) != null && addConsistentMainNodeStr != null) {

                    if ((String)request.getParameter(ImdstDefine.Prop_SubKeyMapNodesInfo) != null && !((String)request.getParameter(ImdstDefine.Prop_SubKeyMapNodesInfo)).equals("")) {

                        if (addConsistentSubNodeStr != null) {

                            if ((String)request.getParameter(ImdstDefine.Prop_ThirdKeyMapNodesInfo) != null && !((String)request.getParameter(ImdstDefine.Prop_ThirdKeyMapNodesInfo)).equals("")) {

                                if (addConsistentThirdNodeStr != null) {
                                    addConsistentNodeStr = addConsistentMainNodeStr + "," + addConsistentSubNodeStr + "," + addConsistentThirdNodeStr;
                                    if(!this.execModParam(imdstKeyValueClient, 
                                                            ImdstDefine.Prop_KeyMapNodesInfo, 
                                                            (String)request.getParameter(ImdstDefine.Prop_KeyMapNodesInfo) + "," + addConsistentMainNodeStr)) 
                                                                throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                                    if(!this.execModParam(imdstKeyValueClient, 
                                                            ImdstDefine.Prop_SubKeyMapNodesInfo, 
                                                            (String)request.getParameter(ImdstDefine.Prop_SubKeyMapNodesInfo) + "," + addConsistentSubNodeStr)) 
                                                                throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                                    if(!this.execModParam(imdstKeyValueClient, 
                                                            ImdstDefine.Prop_ThirdKeyMapNodesInfo, 
                                                            (String)request.getParameter(ImdstDefine.Prop_ThirdKeyMapNodesInfo) + "," + addConsistentThirdNodeStr)) 
                                                                throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                                    if(!this.execModParam(imdstKeyValueClient, 
                                                            ImdstDefine.addNode4ConsistentHashMode, 
                                                            addConsistentNodeStr)) 
                                                                throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                                }
                            } else {
                                addConsistentNodeStr = addConsistentMainNodeStr + "," + addConsistentSubNodeStr;
                                if(!this.execModParam(imdstKeyValueClient, 
                                                        ImdstDefine.Prop_KeyMapNodesInfo, 
                                                        (String)request.getParameter(ImdstDefine.Prop_KeyMapNodesInfo) + "," + addConsistentMainNodeStr)) 
                                                            throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                                if(!this.execModParam(imdstKeyValueClient, 
                                                        ImdstDefine.Prop_SubKeyMapNodesInfo, 
                                                        (String)request.getParameter(ImdstDefine.Prop_SubKeyMapNodesInfo) + "," + addConsistentSubNodeStr)) 
                                                            throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                                if(!this.execModParam(imdstKeyValueClient, 
                                                        ImdstDefine.addNode4ConsistentHashMode, 
                                                        addConsistentNodeStr)) 
                                                            throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                            }
                        }
                    } else {

                        addConsistentNodeStr = addConsistentMainNodeStr;
                        if(!this.execModParam(imdstKeyValueClient, 
                                                ImdstDefine.Prop_KeyMapNodesInfo, 
                                                (String)request.getParameter(ImdstDefine.Prop_KeyMapNodesInfo) + "," + addConsistentMainNodeStr)) 
                                                    throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");
                        if(!this.execModParam(imdstKeyValueClient, 
                                                ImdstDefine.addNode4ConsistentHashMode, 
                                                addConsistentNodeStr)) 
                                                    throw new Exception("Update Param Error Param=[" + (String)request.getParameter("updateparam") + "]");

                    }
                }


            }

            for (int idx = 0; idx < settingParams.length; idx++) {

                clientRet = imdstKeyValueClient.getValue(ImdstDefine.ConfigSaveNodePrefix + settingParams[idx]);
                setting = new HashMap();
                setting.put("param", settingParams[idx]);
                setting.put("value", "&nbsp;");

                // Read Only チェック
                if (settingParams[idx].equals(ImdstDefine.Prop_MainMasterNodeInfo)) {
                    setting.put("readonly", "");
                }

                if (clientRet[0].equals("true")) {
                    if (idx == 0) {
                        if (clientRet[1] != null && !clientRet[1].equals("") && clientRet[1].indexOf(":") != -1) {
                            dataNodeStr = clientRet[1];
                            String[] dataNodeInfos = dataNodeStr.split(",");
                            for (int dni = 0; dni < dataNodeInfos.length; dni++) {
                                String dataNodeDt = imdstKeyValueClient.getDataNodeStatus(dataNodeInfos[dni]);
                                if (dataNodeDt == null) dataNodeDt = "";
                                HashMap dataNodeDtMap = new HashMap(2);
                                dataNodeDtMap.put("name", dataNodeInfos[dni]);
                                dataNodeDtMap.put("dt", dataNodeDt);
                                dataNodeList.add(dataNodeDtMap);
                            }
                        }
                    }

                    if (idx == 1) {
                        if (clientRet[1] != null && !clientRet[1].equals("") && clientRet[1].indexOf(":") != -1) {
                            subNode = true;
                            slaveDataNodeStr = clientRet[1];
                            String[] slaveDataNodeInfos = slaveDataNodeStr.split(",");
                            for (int dni = 0; dni < slaveDataNodeInfos.length; dni++) {
                                String slaveDataNodeDt = imdstKeyValueClient.getDataNodeStatus(slaveDataNodeInfos[dni]);
                                if (slaveDataNodeDt == null) slaveDataNodeDt = "";
                                HashMap slaveDataNodeDtMap = new HashMap(2);
                                slaveDataNodeDtMap.put("name", slaveDataNodeInfos[dni]);
                                slaveDataNodeDtMap.put("dt", slaveDataNodeDt);
                                slaveDataNodeList.add(slaveDataNodeDtMap);
                            }
                        }
                    }

                    if (idx == 2) {
                        if (clientRet[1] != null && !clientRet[1].equals("") && clientRet[1].indexOf(":") != -1) {
                            thirdNode = true;
                            thirdDataNodeStr = clientRet[1];
                            String[] thirdDataNodeInfos = thirdDataNodeStr.split(",");
                            for (int dni = 0; dni < thirdDataNodeInfos.length; dni++) {
                                String thirdDataNodeDt = imdstKeyValueClient.getDataNodeStatus(thirdDataNodeInfos[dni]);
                                if (thirdDataNodeDt == null) thirdDataNodeDt = "";
                                HashMap thirdDataNodeDtMap = new HashMap(2);
                                thirdDataNodeDtMap.put("name", thirdDataNodeInfos[dni]);
                                thirdDataNodeDtMap.put("dt", thirdDataNodeDt);
                                thirdDataNodeList.add(thirdDataNodeDtMap);
                            }
                        }
                    }


                    setting.put("value", clientRet[1]);
                }
                settingList.add(setting);
            }

            
            out.println(initPage(settingList, dataNodeList, slaveDataNodeList, thirdDataNodeList, msg));
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

    private boolean execModParam (ImdstKeyValueClient imdstKeyValueClient, String updateParamKey, String updateParamVal) throws Exception {
        boolean ret = false;
        try {
            if (imdstKeyValueClient.setValue(ImdstDefine.ConfigSaveNodePrefix + updateParamKey, updateParamVal)) ret = true;
        } catch(Exception e) {
            throw e;
        }
        return ret;
        
    }

    private String initPage(ArrayList settingList, ArrayList dataNodeList, ArrayList slaveDataNodeList, ArrayList thirdDataNodeList, String msg) {
        StringBuffer pageBuf = new StringBuffer();
        HashMap keyNodeSetting = (HashMap)settingList.get(0);
        String keyNodeReadOnly = "";

        HashMap subKeyNodeSetting = null;
        String subKeyNodeReadOnly = "";
        HashMap thirdKeyNodeSetting = null;
        String thirdKeyNodeReadOnly = "";

        String postParameterKeys = "KeyMapNodesInfo";

        if (subNode) {
            subKeyNodeSetting = (HashMap)settingList.get(1);
        }

        if (thirdNode) {
            thirdKeyNodeSetting = (HashMap)settingList.get(2);
        }

        HashMap ruleSetting = (HashMap)settingList.get(3);
        HashMap dispatchSetting = (HashMap)settingList.get(4);

        if (subNode == false) {
            subKeyNodeReadOnly = "readonly";
        } else {
            postParameterKeys = postParameterKeys + "#" +"SubKeyMapNodesInfo";
        }

        if (thirdNode == false) {
            thirdKeyNodeReadOnly = "readonly";
        } else {
            postParameterKeys = postParameterKeys + "#" + "ThirdKeyMapNodesInfo";
        }

        if (!((String)dispatchSetting.get("value")).equals("mod")) {
            keyNodeReadOnly = "readonly";
            subKeyNodeReadOnly = "readonly";
            thirdKeyNodeReadOnly = "readonly";
        }

        pageBuf.append("<html>");
        pageBuf.append("  <head>");
        pageBuf.append("    <meta http-equiv='Content-Type' content='text/html; charset=UTF-8' />");
        pageBuf.append("    <title>okuyama manager console</title>");
        pageBuf.append("  </head>");
        pageBuf.append("  <body>");
        pageBuf.append("    <form name='main' method='post' action'/okuyamamgr'>");
        pageBuf.append("      <input type='hidden' name='execmethod' value=''>");
        pageBuf.append("      <input type='hidden' name='updateparam' value=''>");
        pageBuf.append("      <input type='hidden' name='" + (String)ruleSetting.get("param") + "' value='" + (String)ruleSetting.get("value") + "'>");
        pageBuf.append("      <table width=1024px align=center boder=0>");
        pageBuf.append("      <tr>  <td align=center>");
        pageBuf.append("      <h1>Distributed Key Value Store Okuyama Manager Console  </h1>");
        pageBuf.append("      <h2>Manager Display  </h2>");
        pageBuf.append("      <br />");
        pageBuf.append("      <hr />");
        pageBuf.append(msg);
        pageBuf.append("      <table style='width:1024px;' border=1>");
        pageBuf.append("        <tr>");
        pageBuf.append("          <td colspan=2 align=center bgcolor='Silver' width=1024px>");
        pageBuf.append("          Okuyama Setup Information");
        pageBuf.append("          </td>");
        pageBuf.append("        </tr>");

        // KeyNode関係の設定
        pageBuf.append("  <tr>");
        pageBuf.append("    <td width=160px>");
        pageBuf.append("      <p>" + (String)keyNodeSetting.get("param") + "  </p>");
        if (!((String)dispatchSetting.get("value")).equals("mod")) {
            pageBuf.append("      <br>");
        }
        pageBuf.append("    </td>");
        pageBuf.append("    <td width=864px rowspan=4>");
        pageBuf.append("      <table border=0 width=864px>");
        pageBuf.append("        <tr>");
        pageBuf.append("          <td>");
        pageBuf.append("            <input type='text' name='" + (String)keyNodeSetting.get("param") + "' value='" + (String)keyNodeSetting.get("value") + "' size=50 " + keyNodeReadOnly + ">");
        if (!((String)dispatchSetting.get("value")).equals("mod")) {
            pageBuf.append("            <br>Add Main DataNode<input type='text' name='addMainDataNode' value='' size=15>");
        }
        pageBuf.append("          </td>");
        pageBuf.append("        </tr>");
        pageBuf.append("        <tr>");
        pageBuf.append("          <td>");
        if (subNode) {
            pageBuf.append("            <input type='text' name='" + (String)subKeyNodeSetting.get("param") + "' value='" + (String)subKeyNodeSetting.get("value") + "' size=50 " + subKeyNodeReadOnly + ">");
            if (!((String)dispatchSetting.get("value")).equals("mod")) {
                pageBuf.append("            <br>Add Sub DataNode<input type='text' name='addSubDataNode' value='' size=15>");
            }
        } else {
            pageBuf.append("&nbsp;<br>&nbsp;<br>");
        }
        pageBuf.append("          </td>");
        pageBuf.append("        </tr>");
        pageBuf.append("        <tr>");
        pageBuf.append("          <td>");
        if (thirdNode) {
            pageBuf.append("            <input type='text' name='" + (String)thirdKeyNodeSetting.get("param") + "' value='" + (String)thirdKeyNodeSetting.get("value") + "' size=50 " + thirdKeyNodeReadOnly + ">");
            if (!((String)dispatchSetting.get("value")).equals("mod")) {
                pageBuf.append("            <br>Add Third DataNode<input type='text' name='addThirdDataNode' value='' size=15>");
            }
        } else {
            pageBuf.append("&nbsp;<br>&nbsp;<br>");
        }
        pageBuf.append("          </td>");
        pageBuf.append("        </tr>");
        pageBuf.append("        <tr>");
        pageBuf.append("          <td>");
        pageBuf.append("            <input type='text' name='dummyrule' value='" + (String)ruleSetting.get("value") + "' size=15 disabled>");
        if (((String)dispatchSetting.get("value")).equals("mod")) {
            pageBuf.append("            &nbsp;&nbsp;<input type='submit' value='UPDATE' onclick='document.main.execmethod.value=\"modparam\";document.main.updateparam.value=\"" + postParameterKeys + "#" + (String)ruleSetting.get("param") + "\";'>");
        } else {
            pageBuf.append("            &nbsp;&nbsp;<input type='submit' value='UPDATE' onclick='document.main.execmethod.value=\"modparam\";document.main.updateparam.value=\"" + postParameterKeys + "#addMainDataNode#addSubDataNode#addThirdDataNode#" + (String)ruleSetting.get("param") + "\";'>");
        }
        pageBuf.append("          </td>");
        pageBuf.append("        </tr>");
        pageBuf.append("      </table>");
        pageBuf.append("    </td>");
        pageBuf.append("  </tr>");
        pageBuf.append("  <tr>");
        pageBuf.append("    <td width=160px>");
        pageBuf.append("      <p>SubKeyMapNodesInfo</p>");
        if (subNode) {
            if (!((String)dispatchSetting.get("value")).equals("mod")) {
                pageBuf.append("      <br>");
            }
        }
        pageBuf.append("    </td>");
        pageBuf.append("  </tr>");
        pageBuf.append("  <tr>");
        pageBuf.append("    <td width=160px>");
        pageBuf.append("      <p>ThirdKeyMapNodesInfo </p>");
        if (thirdNode) {
            if (!((String)dispatchSetting.get("value")).equals("mod")) {
                pageBuf.append("      <br>");
            }
        }
        pageBuf.append("    </td>");
        pageBuf.append("  </tr>");
        pageBuf.append("  <tr>");
        pageBuf.append("    <td width=160px>");
        pageBuf.append("      <p>" + (String)ruleSetting.get("param") + "  </p>");
        if (!((String)dispatchSetting.get("value")).equals("mod")) {
            pageBuf.append("      <br>");
        }
        pageBuf.append("    </td>");
        pageBuf.append("  </tr>");
        pageBuf.append("  <tr>");
        pageBuf.append("    <td width=160px>");
        pageBuf.append("      <p>" + (String)dispatchSetting.get("param") + "  </p>");
        pageBuf.append("    </td>");
        pageBuf.append("    <td width=864px>");
        pageBuf.append("      <input type='text' name='" + (String)dispatchSetting.get("param") + "' value='" + (String)dispatchSetting.get("value") + "' size=30 disabled>");
        pageBuf.append("    </td>");
        pageBuf.append("  </tr>");



        // それ以外
        for (int i = 5; i < settingList.size(); i++) {
            HashMap setting = (HashMap)settingList.get(i);
            pageBuf.append("  <tr>");
            pageBuf.append("    <td width=160px>");
            pageBuf.append("      <p>" + (String)setting.get("param") + "  </p>");
            pageBuf.append("    </td>");
            pageBuf.append("    <td width=864px>");
            if (setting.get("readonly") == null) {
                pageBuf.append("      <input type='text' name='" + (String)setting.get("param") + "' value='" + (String)setting.get("value") + "' size=50>");
                pageBuf.append("      &nbsp;&nbsp;<input type='submit' value='UPDATE' onclick='document.main.execmethod.value=\"modparam\";document.main.updateparam.value=\"" + (String)setting.get("param") + "\";'>");
            } else{
                pageBuf.append("      <input type='text' name='" + (String)setting.get("param") + "' value='" + (String)setting.get("value") + "' size=50 disabled>");
            }
            pageBuf.append("    </td>");
            pageBuf.append("  </tr>");
        }
        pageBuf.append("      </td>  </tr>");
        pageBuf.append("      </table>");
        pageBuf.append("      <br>");

        if (dataNodeList.size() > 0) {
            pageBuf.append("      <table style='width:1024px;' border=1>");
            pageBuf.append("        <tr>");
            pageBuf.append("          <td colspan=2 align=center bgcolor='Silver' width=1024px>");
            pageBuf.append("            Okuyama DataNode Information");
            pageBuf.append("          </td>");
            pageBuf.append("        </tr>");

            for (int i = 0; i < dataNodeList.size(); i++) {
                HashMap dataNode = (HashMap)dataNodeList.get(i);
                pageBuf.append("  <tr>");
                pageBuf.append("    <td width=160px>");
                pageBuf.append("      <p>" + (String)dataNode.get("name") + "  </p>");
                pageBuf.append("    </td>");
                pageBuf.append("    <td width=864px>");

                String[] nodeDtList = ((String)dataNode.get("dt")).split(";");
                for (int idx = 0; idx < nodeDtList.length; idx++)  {
                    pageBuf.append(nodeDtList[idx]);
                    pageBuf.append("  <br>");
                }

                pageBuf.append("    </td>");
                pageBuf.append("  </tr>");
            }
            pageBuf.append("      </td>  ");
            pageBuf.append("    </tr>  ");
            pageBuf.append("  </table>");
        }

        pageBuf.append("      <br>");
        if (slaveDataNodeList.size() > 0) {
            pageBuf.append("      <table style='width:1024px;' border=1>");
            pageBuf.append("        <tr>");
            pageBuf.append("          <td colspan=2 align=center bgcolor='Silver' width=1024px>");
            pageBuf.append("            Okuyama SlaveDataNode Information");
            pageBuf.append("          </td>");
            pageBuf.append("        </tr>");

            for (int i = 0; i < slaveDataNodeList.size(); i++) {
                HashMap slaveDataNode = (HashMap)slaveDataNodeList.get(i);
                pageBuf.append("  <tr>");
                pageBuf.append("    <td width=160px>");
                pageBuf.append("      <p>" + (String)slaveDataNode.get("name") + "  </p>");
                pageBuf.append("    </td>");
                pageBuf.append("    <td width=864px>");

                String[] nodeDtList = ((String)slaveDataNode.get("dt")).split(";");
                for (int idx = 0; idx  < nodeDtList.length; idx++)  {
                    pageBuf.append(nodeDtList[idx]);
                    pageBuf.append("  <br>");
                }

                pageBuf.append("    </td>");
                pageBuf.append("  </tr>");
            }
            pageBuf.append("      </td>  </tr>");
            pageBuf.append("      </table>");
        }
        pageBuf.append("      </table>");
        pageBuf.append("    </form>");
        pageBuf.append("  </body>");
        pageBuf.append("</html>");
        return pageBuf.toString();
    }
}

