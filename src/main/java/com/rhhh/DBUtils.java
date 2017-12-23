package com.rhhh;

import javax.security.auth.login.FailedLoginException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by root on 25/09/17.
 */
public class DBUtils {
    public static final String DB_URL = "jdbc:mysql://localhost/";
    public static final String RHHH_URL = "jdbc:mysql://localhost/RHHH";
    public static final String USER = "root";
    public static final String PASS = "00123123";
    static final ProcessBuilder db_starter =new ProcessBuilder("service", "mysql", "start");
    static final ProcessBuilder db_stop =new ProcessBuilder("service", "mysql", "start");


    public static Connection ConnectDB() throws FailedLoginException{
        Statement stmt = null;
        Connection conn = null;
        try {
            Process startMysql = db_starter.start();
            startMysql.waitFor();
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.createStatement();
            String sql = "CREATE DATABASE IF NOT EXISTS RHHH;";
            stmt.executeUpdate(sql);
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        return conn;
    }

    public static void createTablesForLevels() throws FailedLoginException {
        Statement stmt;
        try {
            Connection conn = DriverManager.getConnection(RHHH_URL, USER, PASS);
            stmt = conn.createStatement();
            String sql_cmd = "";
            for (int level = 1; level < 5 ; level++) {
                sql_cmd = "DROP TABLE IF EXISTS RHHH.Level" + level;
                stmt.executeUpdate(sql_cmd);
            }
            for (int level = 1; level < 5 ; level++){
                sql_cmd = "CREATE TABLE Level"+ level + " (id BIGINT AUTO_INCREMENT NOT NULL, HH LONGTEXT, total BIGINT UNSIGNED, PRIMARY KEY (id))";
                stmt.executeUpdate(sql_cmd);
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("Success!\n");
    }

    public static void disconnectDB(){
        try {
            Process stopMysql = db_stop.start();
            stopMysql.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}