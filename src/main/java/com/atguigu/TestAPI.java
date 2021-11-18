package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public class TestAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {

            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {


        }
    }
    public static boolean isexist(String tablename) throws IOException {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();

//        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tablename));

//        connection.close();
        return exists;
    }

    public static void createTable(String tableName, String... cfs) throws IOException {
        //判读是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }
        if (isexist(tableName)) {
            System.out.println(tableName + "表已存在");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    public static void dropTable(String Tablename) throws IOException {
        if (!isexist(Tablename)) {
            System.out.println(Tablename + "表不存在！！！");
            return;
        }
        admin.disableTable(TableName.valueOf(Tablename));
        admin.deleteTable(TableName.valueOf(Tablename));
    }

    public static void createNameSpace(String ns) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(ns+"命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putdata(String tablename, String rowkey, String cf, String cn, String value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        table.put(put);

        table.close();
    }

    public static void getdata(String tablename, String rowkey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("RW:" + Bytes.toString(CellUtil.cloneRow(cell))+"CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "VALUE" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void scanTable(String tablename) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan();
        byte[] startRow = scan.getStartRow();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RW:" + Bytes.toString(CellUtil.cloneRow(cell)) + "CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }
    }

    public static void deleteTable(String tablename, String rowkey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        table.delete(delete);
        table.close();
    }
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {

//        System.out.println(isexist("stu5"));
//        createTable("stu", "info");
//        dropTable("stu5");
//        System.out.println(isexist("stu5"));
//        createNameSpace("0408");
        putdata("stu", "1001", "info", "sex", "zhangsan");
//        getdata("stu","1005","info","name");
        close();
    }
}
