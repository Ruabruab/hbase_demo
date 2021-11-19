package com.atguigu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


import java.io.IOException;
import java.util.List;

/**
 * @author 公羽
 * @time : 2021/4/11 15:02
 * @File : HBase_test.java
 */
public class test {
    private Connection connection;
    private Admin admin;

    @BeforeTest
    public void beforeTest() throws IOException {
//        1.创建hbase配置
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

//        2.创建hbase的连接
        connection = ConnectionFactory.createConnection(configuration);
//        3.创建admin的连接
        admin = connection.getAdmin();
    }

    //        测试创建表
    @Test
    public void createTableTest() throws IOException {
//        1.判断表是否存在
        TableName tableName = TableName.valueOf("water_bill");
        if (admin.tableExists(tableName)) {
            return;
        }
//        2.构建表描述构建器(构建器设计模式)
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
//        3.构建列族描述构建器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
//        4.构建表构建器和列族描述构建器建立联系
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
//        5.创建表
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
    }

    //       测试删除表
    @Test
    public void deleteTableTest() throws IOException {
//        1.确认表是否存在
        TableName tableName = TableName.valueOf("water_bill");
        if (admin.tableExists(tableName)) {
//            禁用表
            admin.disableTable(tableName);
//            删除表
            admin.deleteTable(tableName);
        }
    }

    //    测试插入数据
    @Test
    public void putTableTest() throws IOException {
//        1.使用hbase连接获取表
        TableName tableName = TableName.valueOf("water_bill");
        Table table = connection.getTable(tableName);
//        2.构建rowkey,列簇名,列名
//        3.构建put对象
        Put put = new Put(Bytes.toBytes("4944191"));
//        4.添加姓名列
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
//        5.使用表执行put操作
        table.put(put);
//        6.关闭表对象
        table.close();
    }

    //测试查看数据
    @Test
    public void getableTest() throws IOException {
//        1、使用hbase连接获取表
        TableName tableName = TableName.valueOf("water_bill");
        Table table = connection.getTable(tableName);
//        2、构建rowkey、列簇名、列名
//        3、构建get对象
        Get get = new Get(Bytes.toBytes("4944191"));
//        4、执行get请求，获取result对象
        Result result = table.get(get);
//        5、获取所有的单元格
        List<Cell> cellList = result.listCells();
        for (Cell cell : cellList) {
//          获取单元格的列簇名
            String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
//          获取单元格的列名
            String cn = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
//          获取单元格的值
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(cf + ":" + cn + "->" + value);
        }
//        6、关闭表对象
        table.close();
        System.out.println("hello");
        System.out.println("world");
        System.out.println("hot-fix");
        System.out.println("master");
        System.out.println("hot");
        System.out.println("gitee");
        System.out.println("fail");
    }

    @AfterTest
    public void afterTest() throws IOException {
//        4.关闭admin
        admin.close();
        connection.close();
    }
}

