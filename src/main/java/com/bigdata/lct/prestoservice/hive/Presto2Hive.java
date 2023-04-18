package com.bigdata.lct.prestoservice.hive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Presto2Hive  implements Runnable{

    public static void main(String[] args)  throws Exception {
        Presto2Hive tr = new Presto2Hive();
        Thread tr1 = new Thread(tr,"query01");
        Thread tr2 = new Thread(tr,"query02");
        Thread tr3 = new Thread(tr,"query03");
        Thread tr4 = new Thread(tr,"query04");
        Thread tr5 = new Thread(tr,"query05");
        Thread tr6 = new Thread(tr,"query06");
        Thread tr7 = new Thread(tr,"query07");
        Thread tr8 = new Thread(tr,"query08");
        Thread tr9 = new Thread(tr,"query09");
        Thread tr10 = new Thread(tr,"query10");
        tr1.start();
        tr2.start();
        tr3.start();
        tr4.start();
        tr5.start();
        tr6.start();
        tr7.start();
        tr8.start();
        tr9.start();
        tr10.start();
    }


//    通过presto查询hive中表的数据
    public static void  presto2HiveQuery(String sql_Str) throws Exception {
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
//        访问presto的客户端：./presto_cli --server 192.168.12.41:8089 --catalog hive
//        192.168.12.41 节点使presto集群的协调节点
        String url = "jdbc:presto://192.168.12.41:8089/hive";
//        这里可以增加一个用户名密码
        Connection connection = DriverManager.getConnection(url, "hisign", null);
        Statement stmt = connection.createStatement();
//        String sql = "select * from xzxtods.tb01 t1 join xzxtods.tb02 t2 on t1.id=t2.id limit 100";
        String sql =sql_Str;
        stmt.execute("use xzxtods");
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        while (rs.next()) {
            for (int i = 90; i <= columnCount; i++) {
                System.out.println(md.getColumnName(i)+","+rs.getObject(i));
            }
        }
//      关闭资源
        rs.close();
        connection.close();
    }

//    通过presto向hive中表插入数

//    通过presto在hive中创建表
    public void presto2HiveCreate(){

    }
//    通过presto在hive中删除表

//读取查询条件
    public static List<String> readFile(String inputPath){
        List<String> lines = new ArrayList<String>();
        try {
            //按照UTF-8的字符编码读取
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputPath)),"UTF-8"));
            String line = null;
            while((line=br.readLine())!=null){
                lines.add(line);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lines;
    }

    @Override
    public void run() {
        List<String> sql_list = readFile("E:\\临时文件\\2023-03-23\\testsql.txt");
        for(int i=0;i<100;i++){
            for(int j=0;j<sql_list.size();j++){
                String sql = sql_list.get(j);
                try {
                    presto2HiveQuery(sql);
                    //延迟一秒发起下一个查询
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
