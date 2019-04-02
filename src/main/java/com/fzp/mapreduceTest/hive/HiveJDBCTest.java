package com.fzp.mapreduceTest.hive;


import java.sql.*;

public class HiveJDBCTest {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = DriverManager.getConnection("jdbc:hive2://node02:10000/mytest","root","");
        Statement stat = connection.createStatement();
        String sql = "select * from abc";
        ResultSet result = stat.executeQuery(sql);
        while (result.next()){
            System.out.println(result.getString(0)+"--"+result.getString("name"));
        }

    }

}
