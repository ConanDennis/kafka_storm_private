package byr.bolt.stat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import byr.common.Constants;
import org.apache.commons.lang3.StringUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import byr.utils.DateUtils;
import redis.clients.jedis.Jedis;

/**
 * 以天为单位，统计网站每天访问频率,数据量为1周（每天）
 */
public class WebSiteEveryDayBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -7536333499541801989L;
    private Map<String,Integer> countMap;
    private Jedis jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        countMap = new ConcurrentHashMap<>();
        jedis = new Jedis(stormConf.get(Constants.REDIS_HOST).toString(),((Long)stormConf.get(Constants.REDIS_PORT)).intValue());
        jedis.select(7);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String time = tuple.getString(2);
        String req = tuple.getString(3);
        if(StringUtils.isNotBlank(time) && "/index.php".equals(req)){
            String date = DateUtils.splitYearMonthAndDay(time);
            if(countMap.containsKey(date)){
                countMap.put(date,countMap.get(date) + 1);
            }else{
                countMap.put(date,1);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId","flag","time","req"));
    }

    public void cleanup() {
        System.out.println("============cleanup==============");
        Set<String> keySet = this.countMap.keySet();
        if(keySet != null){
            ArrayList<String> keys = new ArrayList<>(keySet);
            Collections.sort(keys);
            String data = "";
            StringBuffer sb = new StringBuffer();
            sb.append("{");
            sb.append("\"category\":[");
            for (String key : keys){
                sb.append("\""+key+"\",");
                data += this.countMap.get(key) + ",";
            }
            if(sb.toString().endsWith(",")){
                sb.delete(sb.length() - 1,sb.length());
            }
            if(data.endsWith(",")){
                data = data.substring(0, data.length() - 1);
            }
            sb.append("],");
            sb.append("\"data\": [[");
            sb.append(data);
            sb.append("]],");
            sb.append("\"type\": \"line\", \"legend\": [\"主页\"], \"title\": \"网站每天访问量\"");
            sb.append("}");
            System.out.println("最终结果: " + sb.toString());
            Map<String,String> map = new HashMap<>();
            map.put(Constants.EVERYDAY_COUNT_KEY,sb.toString());
            jedis.hmset(Constants.EVERYDAY_COUNT_MAP, map);
        }
//        Iterator<Map.Entry<String,Integer>> iter = this.countMap.entrySet().iterator();
//        while (iter.hasNext()) {
//            Map.Entry<String, Integer> entry = iter.next();
//            System.out.println(entry.getKey() + "============" + entry.getValue());
//        }
    }

}
