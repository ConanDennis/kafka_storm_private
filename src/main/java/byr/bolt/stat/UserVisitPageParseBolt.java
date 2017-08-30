package byr.bolt.stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import byr.common.Constants;
import redis.clients.jedis.Jedis;

/**
 * 每个用户访问每个页面的频率解析
 */
public class UserVisitPageParseBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 7376410128999610762L;
    //Map<userName,Map<pageId,count>>
    private Map<String,Map<String,Double>> countMap;
    private Jedis jedis;
    private Set<String> pageSet;
    private BufferedWriter bufferedWriter;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        countMap = new ConcurrentHashMap<>();
        jedis = new Jedis(stormConf.get(Constants.REDIS_HOST).toString(),((Long)stormConf.get(Constants.REDIS_PORT)).intValue());
        jedis.select(7);
        pageSet = new HashSet<>();
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(Constants.cluster_filter));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String parseUrl(String req){
//        System.out.println("parseUrl===req: "+req);
        if(StringUtils.isBlank(req)){
            return req;
        }
        if(req.contains("?")){
            return req.substring(0,req.indexOf("?"));
        }
        return req;
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String userId = tuple.getString(0);
        String req = parseUrl(tuple.getString(3));
        if(Constants.cluster_urls.contains(req) || Constants.matcher(req)){
            pageSet.add(req);
            Map<String,Double> pageCountMap = countMap.get(userId);
            if(pageCountMap == null){
                pageCountMap = new HashMap<>();
            }
            pageCountMap.put(req,pageCountMap.get(req) == null ? 1 : pageCountMap.get(req) + 1);
            countMap.put(userId,pageCountMap);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void cleanup() {
        System.out.println("============cleanup==============");
//        Iterator<Map.Entry<String, Map<String, Double>>> iterator = this.countMap.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Map<String, Double>> next = iterator.next();
//            String key = next.getKey();
//            Map<String, Double> value = next.getValue();
//            Iterator<Map.Entry<String, Double>> iterator1 = value.entrySet().iterator();
//            while (iterator1.hasNext()){
//                Map.Entry<String, Double> next1 = iterator1.next();
//                System.out.println(key + "======" +next1.getKey()+"====="+next1.getValue());
//            }
//        }
//        System.out.println("============cleanup=end=============");

        System.out.println("=================cluster start===================");
        Map<String,List<Double>> result = new HashMap<>();
        Set<String> userList = this.countMap.keySet();
        Map<String, Double> pageCount;
        List<Double> values;
        for (String userName : userList){
            values = new ArrayList<>();
            pageCount = this.countMap.get(userName);
            for(String url : Constants.cluster_urls){
                if(url.equals("jc")){
                    Set<Map.Entry<String, Double>> entries = pageCount.entrySet();
                    double n = 0;
                    for (Map.Entry<String, Double> key : entries){
                       if(key.getKey().startsWith("/jc")){
                          n +=  key.getValue() == null ? 0 : key.getValue();
                       }
                    }
                    values.add(n);
                }else{
                    values.add(pageCount.get(url) == null ? (double)0 : pageCount.get(url));
                }
            }
            //归一化处理
            Double max = Collections.max(values);
            if(max != null && max.doubleValue() > 0){
                List<Double> collect = values.stream().map(v -> max == 0 ? 0 : (v / max)).collect(Collectors.toList());
                result.put(userName,collect);
            }else{
                result.put(userName,values);
            }
//            result.put(userName,values);
        }
        Set<Map.Entry<String, List<Double>>> entries = result.entrySet();
        for (Map.Entry<String, List<Double>> map : entries){
            try {
                bufferedWriter.write(map.getKey() + "====$$$$===" + map.getValue()+"\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
//            System.out.println(map.getKey() + "====$$$$===" + map.getValue());
        }
        System.out.println("=================cluster end====================="+result.size());
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
