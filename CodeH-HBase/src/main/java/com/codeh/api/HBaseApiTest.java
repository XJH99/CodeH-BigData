package com.codeh.api;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ApiTest
 * @date 2021/5/12 15:49
 * @description api功能测试
 */
public class HBaseApiTest {

    public static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;

    // 初始化HBase的client连接
    static {
        try {
            // 1.获取配置对象
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

    public static void main(String[] args) throws IOException {
//        System.out.println(isTableExist("stu1"));

//        createTable("stu1", "info1", "info2");
//        dropTable("stu1");

//        System.out.println(isTableExist("stu1"));
//        createNameSpace("codeh");
//        putData("student", "1003", "info", "name", "liuKe");

//        getData("student", "1002", "info", "name");
//        scanTable("student");
        deleteData("student", "1003", "info", "");
        // 关闭资源
        close();
    }

    /**
     * 删除数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     */
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        // 1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey)); // 指定rowKey是删除多个版本

        // 2.1给delete对象添加条件
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn)); // 多个版本的删除
        //delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));  // 单个版本的删除，这个API在生产环境慎用

        // 3.删除数据
        table.delete(delete);

        // 4.关闭连接
        table.close();
    }

    /**
     * 获取数据 scan
     *
     * @param tableName
     */
    public static void scanTable(String tableName) throws IOException {
        // 1.获取表连接对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.创建scan对象
        Scan scan = new Scan();

        // 3.扫描表
        ResultScanner scanner = table.getScanner(scan);

        // 4.解析scanner对象
        for (Result result : scanner) {

            // 5.解析result
            for (Cell cell : result.rawCells()) {
                // 6.打印数据
                System.out.println("rowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
                        "CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                        "cn:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                        "value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        // 7.关闭表连接
        table.close();
    }


    /**
     * 获取数据 get
     *
     * @param tableName
     * @param rowKey
     * @param cf        列簇
     * @param cn        列
     */
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        // 1.获取表连接对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.获取Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 2.1指定列簇信息
//        get.addFamily(Bytes.toBytes(cf));
        // 2.2指定列簇与列的信息
        //get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        // 2.3设置版本
        get.getMaxVersions();

        // 3.获取数据
        Result result = table.get(get);

        // 4.解析result并进行打印
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                    "cn:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                    "value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        // 5.关闭表资源
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        // 调用API判断表是否存在
        boolean flag = admin.tableExists(TableName.valueOf(tableName));
        return flag;
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param cfs       列簇
     */
    public static void createTable(String tableName, String... cfs) throws IOException {
        // 1.判断表是否已存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        }

        // 2.判断是否存在列簇信息
        if (cfs.length <= 0) {
            System.out.println("请设置列簇信息");
            return;
        }

        // 3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : cfs) {
            // 4.创建列簇描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            // 5.添加列簇
            hTableDescriptor.addFamily(hColumnDescriptor);
        }


        // 创建表
        admin.createTable(hTableDescriptor);

    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)) {
            System.out.println("表" + tableName + "不存在");
        }

        // 1.使表下线
        admin.disableTable(TableName.valueOf(tableName));

        // 2.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建命名空间
     *
     * @param ns
     */
    public static void createNameSpace(String ns) {

        // 1.创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();

        // 2.创建命名空间
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间" + ns + "已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     *
     * @param tableName    表名
     * @param rowKey       行键
     * @param columnFamily 列簇
     * @param col          列
     * @param value        值
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String col, String value) throws IOException {
        // 1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(col), Bytes.toBytes(value));

        // 2.插入数据
        table.put(put);

        table.close();

        System.out.println("插入数据成功~~");
    }

    /**
     * 关闭资源
     */
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
