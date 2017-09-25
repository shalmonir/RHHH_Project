package com.rhhh;

import clojure.lang.Range;

import javax.security.auth.login.FailedLoginException;
import java.io.IOException;
import java.sql.*;

/**
 * Created by root on 25/09/17.
 */
public class DBUtils {
    // used: sudo apt-get install mysql-server
    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
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
        Statement stmt = null;
        try {
            Connection conn = DriverManager.getConnection(RHHH_URL, USER, PASS);
            stmt = conn.createStatement();
            String sql_cmd = "";
            for (int level = 1; level < 5 ; level++) {
                sql_cmd = "DROP TABLE IF EXISTS RHHH.Level" + level;
                stmt.executeUpdate(sql_cmd);
            }
            for (int level = 1; level < 5 ; level++){
                //sql_cmd = "CREATE TABLE Level"+ level + " (Level varchar(256), HHCounterSerialized LONGTEXT,PRIMARY KEY (Level))";
                //sql_cmd = "CREATE TABLE Level"+ level + " (ip varchar(50), count int, primary key (ip))";
                sql_cmd = "CREATE TABLE Level"+ level + " (HH LONGTEXT)";
                stmt.executeUpdate(sql_cmd);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Success!\n");
    }

    public static void DBInit(){
        try {
            ConnectDB();
            createTablesForLevels();
        } catch (FailedLoginException e) {
            e.printStackTrace();
            System.out.println("Failed to connect to db");
            System.exit(1);
        }
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

    public static long writeJavaObject(Connection conn, Object object, String write_cmd) throws Exception {
        String className = object.getClass().getName();
        PreparedStatement pstmt = conn.prepareStatement(write_cmd);
        pstmt.setString(1, className);
        pstmt.setObject(2, object);
        pstmt.executeUpdate();
        ResultSet rs = pstmt.getGeneratedKeys();
        int id = -1;
        if (rs.next()) {
            id = rs.getInt(1);
        }
        rs.close();
        pstmt.close();
        System.out.println("writeJavaObject: done serializing: " + className);
        return id;
    }

    public static Object readJavaObject(Connection conn, long id, String read_cmd) throws Exception {
        PreparedStatement pstmt = conn.prepareStatement(read_cmd);
        pstmt.setLong(1, id);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        Object object = rs.getObject(1);
        String className = object.getClass().getName();
        rs.close();
        pstmt.close();
        System.out.println("readJavaObject: done de-serializing: " + className);
        return object;
    }
}