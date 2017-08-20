package byr.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ReadLogSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader br;
    private String log_name = "D:\\Storm项目相关资料\\log\\temp\\{bt.byr.cn.1.log}.txt";

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            this.br = new BufferedReader(new InputStreamReader(new FileInputStream(log_name), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public void activate() {
        // TODO Auto-generated method stub

    }

    public void deactivate() {
        // TODO Auto-generated method stub

    }

    public void nextTuple() {
        Utils.sleep(100L);
        String str = "";
        try {
            while ((str = this.br.readLine()) != null) {
                this.collector.emit(new Values(str));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
