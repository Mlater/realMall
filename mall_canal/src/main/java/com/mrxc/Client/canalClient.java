package com.mrxc.Client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mrxc.constants.MallConstants;
import com.mrxc.util.MyKafkaUtil;

import java.net.InetSocketAddress;
import java.util.Random;

public class canalClient {
    public static void main(String[] args) {
        //1、获取Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("master102", 11111),
                "example",
                "", "");

        //2、有了连接就不断的开始抓取数据，并进行解析
        while (true) {
            //2、连接器连接Canal
            canalConnector.connect();

            //3、监控mall数据库的所有表
            canalConnector.subscribe("mall.*");

            /**
             * 对象名称	介绍	包含内容
             * 一个message包含多个 sql(event)=Entry集合
             * entry 对应一个sql命令，一个sql可能会对多行记录造成影响。	序列化的数据内容storeValue
             * rowchange 是把entry中的storeValue反序列化的对象。
             * 	1 rowdatalist  行集
             * 2 eventType(数据的变化类型  insert update delete create  alter drop)
             * RowData  出现变化的数据行信息  1 afterColumnList (修改后)     2 beforeColumnList（修改前）
             * 	一个RowData里包含了多个column，每个column包含了 name和 value	1 columnName    2 columnValue
             */

            //4、抓取数据,100条数据（执行sql变化的数据）为一个批次，当然每次抓取如果不够100，有多少抓多少
            Message message = canalConnector.get(100);

            //5、因为抓取的数据可能为空，(也就是可能没有变化)所以先进行判空
            if (message.getEntries().size() <= 0) {
                System.out.println("暂时没有数据");
                try {
                    //没有数据就休息5秒
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //有数据，message拿到的是一个Entry集合，所以我们遍历一下
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //一个Entry对应的是一条sql的执行结果，我们只需要操作数据的sql的执行结果
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            //先进行反序列化,rowchange 是把entry中的storeValue反序列化的对象。
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            //获取表名
                            String tableName = entry.getHeader().getTableName();
                            //拿到事件类型,要按照不同的类型放到kafka不同的topic
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //把数据发送到kafka
                            handle(tableName, eventType, rowChange);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {
        //筛选出订单表的数据来,并且 事件类型必须是新增及变化的
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //发送的订单表
            sendToKafka(rowChange,MallConstants.Mall_ORDER_INFO_TOPIC);
        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //订单详情表
            sendToKafka(rowChange,MallConstants.Mall_ORDER_DETAIL_TOPIC);
        }else if ("user_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //用户表
            sendToKafka(rowChange,MallConstants.Mall_USER_INFO_TOPIC);
        }
    }

    private static void sendToKafka( CanalEntry.RowChange rowChange,String topic) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
        //把获取的变化的数据遍历一下，是一个RowDataList，包含多行
            //创建JSON对象，把一行的数据封装进去
            JSONObject jsonObject = new JSONObject();

            //对每一行的列进行遍历，形成K，V，并进行封装进json对象中
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //打印测试一下
            System.out.println(jsonObject.toString());
            try {
                Thread.sleep(new Random().nextInt(5) * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //发送到Kafka
            MyKafkaUtil.send(topic,jsonObject.toString());
        }
    }
}
