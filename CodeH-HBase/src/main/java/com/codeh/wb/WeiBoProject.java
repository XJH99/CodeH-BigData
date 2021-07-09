package com.codeh.wb;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className WeiBoProject
 * @date 2021/5/13 18:13
 * @description 谷粒微博项目实现
 */
public class WeiBoProject {

    public static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop105,hadoop103,hadoop104");
            // 2.获取连接对象
            connection = ConnectionFactory.createConnection(conf);

            // 3.获取HBase的Client连接对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 初始化操作
        init();

        // 创建content表
        createTable(Constant.WB_TABLE_CONTENT, 1, "info");

        // 创建用户关系表
        createTable(Constant.WB_TABLE_RELATIONS, 1, "attends", "fans");

        // 创建微博收件箱表
        createTable(Constant.WB_TABLE_RECEIVE_EMAIL, 1000, "info");


    }


    /**
     * 创建命名空间
     */
    public static void init() {
        try {
            NamespaceDescriptor build = NamespaceDescriptor
                    .create("weibo")
                    .addConfiguration("create", "codeH")
                    .addConfiguration("create_time", System.currentTimeMillis() + "")
                    .build();
            admin.createNamespace(build);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param version
     * @param cfs
     */
    public static void createTable(String tableName, Integer version, String... cfs) {
        try {
            // 创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(cf));

                hColumnDescriptor.setBlockCacheEnabled(true);
                hColumnDescriptor.setBlocksize(2097152);
                hColumnDescriptor.setMaxVersions(version);
                hColumnDescriptor.setMinVersions(version);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发布微博
     * @param uid
     * @param content
     */
    public static void publishContent(String uid, String content) {
        try {
            Table table = connection.getTable(TableName.valueOf(Constant.WB_TABLE_CONTENT));

            // 获取当前时间戳
            long timestamp = System.currentTimeMillis();

            // 组装rowKey
            String rowKey = uid + "_" + timestamp;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("content"), timestamp, Bytes.toBytes(content));

            table.put(put);

            // 向微博收件箱表中加入发布的RowKey
            Table relationsTable = connection.getTable(TableName.valueOf(Constant.WB_TABLE_RELATIONS));

            // 取出目标数据
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes("fans"));

            Result result = relationsTable.get(get);

            List<byte[]> fans = new ArrayList<byte[]>();

            // 遍历取出当前发布微博用户的所有粉丝数据
            for (Cell cell: result.rawCells()) {
                fans.add(CellUtil.cloneQualifier(cell));
            }

            // 如果该用户没有粉丝，直接返回
            if (fans.size() <= 0) return;

            // 收件箱表操作
            Table emailTable = connection.getTable(TableName.valueOf(Constant.WB_TABLE_RECEIVE_EMAIL));

            List<Put> puts = new ArrayList<Put>();
            for (byte[] fan: fans) {
                Put fanPut = new Put(fan);
                fanPut.add(Bytes.toBytes("info"), Bytes.toBytes(uid), timestamp, Bytes.toBytes(rowKey));
                puts.add(fanPut);
            }
            emailTable.put(puts);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
