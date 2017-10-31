package byr.bolt;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogFilterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 8877743243063240108L;
    private String regular = "([a-z]{4}\\d{2})\\|([1,2])[\\s\\-]+\\[(\\d{1,2}/[a-zA-z]+/\\d{4}:\\d{2}:\\d{2}:\\d{2})\\s\\+\\d+\\]\\s+\"(GET|POST)\\s+(/[^\\s]+)\\s[^\"]*\"";
    private Pattern pattern = Pattern.compile(regular);

    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        //        System.out.println("处理当前行===:"+line);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()){
            String userId = matcher.group(1);//wjif03
            String flag = matcher.group(2);//2
            String time = matcher.group(3);//#27/Jul/2017:23:42:18
            String req = matcher.group(5);//#/shoutbox.php?type=shoutbox
            System.out.println("######userId: "+userId+"====flag: "+flag+"====time: "+time+"====req: "+req);
            if(StringUtils.isBlank(userId) || StringUtils.isBlank(flag) || StringUtils.isBlank(time) || StringUtils.isBlank(req)){
                System.out.println("userId: "+userId+"====flag: "+flag+"====time: "+time+"====req: "+req);
            }
            if(StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(req) && req.contains("php")){
                collector.emit(new Values(userId,flag,time,req));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId","flag","time","req"));
    }
    public void cleanup() {
    }
}
