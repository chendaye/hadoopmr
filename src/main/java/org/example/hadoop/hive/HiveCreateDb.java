package org.example.hadoop.hive;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveCreateDb {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //todo: 账号密码 默认是 mysql的密码
        Connection conn = DriverManager.getConnection(
                "jdbc:hive2://localhost:10000/learn", "hadoop", "hadoop");
        Statement stmt = conn.createStatement();
        String sql = " select * from emp";
        ResultSet resultSet = stmt.executeQuery(sql);
        while (resultSet.next()){
            System.out.println(resultSet.getString(2));

        }
        conn.close();
    }
}
