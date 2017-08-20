package byr.bolt.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import byr.common.Constants;
import byr.utils.DateUtils;
import redis.clients.jedis.Jedis;

/**
 * 以小时为单位，按照身份进行统计（数据量为一天的数据）
 * 教师 1
 * 学生 2
 * redis-Map<date,data>
 */
public class WebSiteIdentityBolt extends BaseBasicBolt {

    //Map<日期,Map<时间,值>>
    private Map<String,Map<String,Integer>> teacherMap;
    private Map<String,Map<String,Integer>> studentMap;
    private Jedis jedis;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        studentMap = new ConcurrentHashMap<>();
        teacherMap = new ConcurrentHashMap<>();
        jedis = new Jedis(stormConf.get(Constants.REDIS_HOST).toString(),((Long)stormConf.get(Constants.REDIS_PORT)).intValue());
        jedis.select(7);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String flag = tuple.getString(1);
        String time = tuple.getString(2);
        if(StringUtils.isBlank(time)) {
            return;
        }
        String[] split = time.split(":");
        if(split == null || split.length != 4){
            return;
        }
        String year = split[0];
        String cTime = split[1]+":"+split[2]+":"+split[3];
        String range = DateUtils.getCurrentTimeRange(cTime);
        System.out.println("=====range====="+cTime+"====="+range);
        Map<String,Integer> map;
        if(flag.equals("1")){
            if(!teacherMap.containsKey(year)){
                map = new ConcurrentHashMap<>();
            }else{
                map = teacherMap.get(year);
            }
            map.put(range,map.get(range) == null ? 1 : map.get(range) + 1);
            teacherMap.put(year,map);
        }else if(flag.equals("2")){
            if(!studentMap.containsKey(year)){
                map = new ConcurrentHashMap<>();
            }else{
                map = studentMap.get(year);
            }
            map.put(range,map.get(range) == null ? 1 : map.get(range) + 1);
            studentMap.put(year,map);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("userId","flag","time","req"));
    }

    private void print(Map<String,Map<String,Integer>> dataMap){
        Iterator<Map.Entry<String, Map<String, Integer>>> iterator = dataMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, Integer>> next = iterator.next();
            Set<Map.Entry<String, Integer>> entries = next.getValue().entrySet();
            String year = next.getKey();
            Iterator<Map.Entry<String, Integer>> iterator1 = entries.iterator();
            while (iterator1.hasNext()){
                Map.Entry<String, Integer> next1 = iterator1.next();
                System.out.println("print===="+year + "===" + next1.getKey()+"==="+next1.getValue());
            }
        }
    }

    private Map<String,String> builderCharData(){
        Map<String,String> result = new HashMap<>();
        Set<String> allDate = new HashSet<>();
        Set<String> keySet = studentMap.keySet();
        Set<String> keySet1 = teacherMap.keySet();
        if(keySet != null && keySet.size() > 0){
            allDate.addAll(keySet);
        }
        if(keySet1 != null && keySet1.size() > 0){
            allDate.addAll(keySet1);
        }
        List<String> dates = new ArrayList<>(allDate);
        Collections.sort(dates);
        Map<String, Integer> stuMap;
        Map<String, Integer> teaMap;
        StringBuffer root = new StringBuffer();
        StringBuffer stuBuf = new StringBuffer();
        StringBuffer teaBuf = new StringBuffer();
        List<String> timeSpanList = DateUtils.buildTimeList();
        for (String date : dates){
            stuMap = studentMap.get(date);
            teaMap = teacherMap.get(date);
            root.append("{");
            root.append("\"category\":[");
            stuBuf.append("[");
            teaBuf.append("[");
            for (String currTime : timeSpanList){
                root.append("\""+currTime+"\",");
                stuBuf.append((stuMap == null || stuMap.get(currTime) == null ? "0" : stuMap.get(currTime)) + ",");
                teaBuf.append((teaMap == null || teaMap.get(currTime) == null ? "0" : teaMap.get(currTime)) + ",");
            }
            stuBuf = del(stuBuf);
            teaBuf = del(teaBuf);
            stuBuf.append("]");
            teaBuf.append("]");
            root = del(root);
            root.append("],");
            root.append("\"data\":[");
            root.append(teaBuf.toString()+",");
            root.append(stuBuf.toString());
            root.append("],");
            root.append("\"legend\":[\"教师\",\"学生\"],");
            root.append("\"title\":\"身份统计\",");
            root.append("\"type\":\"line\"");
            root.append("}");
            result.put(date,root.toString());
            teaBuf.delete(0,teaBuf.length());
            stuBuf.delete(0,stuBuf.length());
            root.delete(0,root.length());
        }
        return result;
    }

    private StringBuffer del(StringBuffer sb){
        if(sb != null && sb.toString().endsWith(",")){
            return sb.delete(sb.length() - 1, sb.length());
        }
        return sb;
    }

    public void cleanup() {
        System.out.println("==================111111111=========================");
        print(studentMap);
        System.out.println("==================222222222=========================");
        print(teacherMap);
        System.out.println("==================333333333=========================");
        Map<String, String> map = builderCharData();
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> data : entries){
            System.out.println("**********"+data.getKey()+"******"+data.getValue());
        }
        jedis.hmset(Constants.IDENTITY_MAP, map);
        System.out.println("==================44444444444=========================");
    }
}
