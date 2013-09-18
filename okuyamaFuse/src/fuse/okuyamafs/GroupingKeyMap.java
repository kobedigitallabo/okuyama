package fuse.okuyamafs;

import java.util.*;

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class GroupingKeyMap {

    private Map dataMap = null;

    private Object sync = new Object();


    public GroupingKeyMap() {
        dataMap = new HashMap();
    }


    public void addGroupingData(Object groupKey, Object tartgetValue) {

        synchronized(sync) {

            LinkedHashMap nowGroupMap = (LinkedHashMap)dataMap.get(groupKey);
            LinkedHashMap newGroupMap = new LinkedHashMap();

            if (nowGroupMap != null) {

                Iterator ite = nowGroupMap.entrySet().iterator();
                while(ite.hasNext()) {
                    Map.Entry entry = (Map.Entry)ite.next();
                    newGroupMap.put(entry.getKey(), entry.getValue());
                }
            }
            newGroupMap.put(tartgetValue, null);
            dataMap.put(groupKey, newGroupMap);
        }
    }


    public List getGroupingData(Object groupKey) {
        List retGroupList = null;

        synchronized(sync) {

            LinkedHashMap nowGroupMap = (LinkedHashMap)dataMap.get(groupKey);

            if (nowGroupMap != null) {

                Iterator ite = nowGroupMap.entrySet().iterator();
                retGroupList = new ArrayList();

                while(ite.hasNext()) {
                    Map.Entry entry = (Map.Entry)ite.next();
                    retGroupList.add(entry.getKey());
                }
            }
        }
        return retGroupList;
    }


    public List removeGroupingData(Object groupKey) {
        List retGroupList = null;

        synchronized(sync) {
            LinkedHashMap nowGroupMap = (LinkedHashMap)dataMap.remove(groupKey);

            if (nowGroupMap != null) {

                Iterator ite = nowGroupMap.entrySet().iterator();
                retGroupList = new ArrayList();

                while(ite.hasNext()) {
                    Map.Entry entry = (Map.Entry)ite.next();
                    retGroupList.add(entry.getKey());
                }
            }
        }
        return retGroupList;
    }
}