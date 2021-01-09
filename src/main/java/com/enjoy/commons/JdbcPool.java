package com.enjoy.commons;

import com.enjoy.conf.ConfManager;
import com.enjoy.constants.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class JdbcPool {

    private static JdbcPool instance;
    private LinkedList<Connection> jdbcPool = new LinkedList<Connection>();

    static {
        try {
            Class.forName(ConfManager.getStrValue(Constants.JDBC_DRIVER));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private JdbcPool() {

        int poolSize = ConfManager.getIntValue(Constants.JDBC_SIZE);
        String url = ConfManager.getStrValue(Constants.JDBC_URL);
        String user = ConfManager.getStrValue(Constants.JDBC_USER);
        String password = ConfManager.getStrValue(Constants.JDBC_PASSWORD);
        for (int i = 0; i < poolSize; i++) {
            try {
                Connection conn = DriverManager.getConnection(url,user,password);
                jdbcPool.add(conn);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public static JdbcPool getInstance(){
        if (instance == null) {
            synchronized (JdbcPool.class) {
                if (instance == null) {
                    instance = new JdbcPool();
                }
            }
        }
        return instance;
    }

    public synchronized Connection getConn(){

        while (jdbcPool.size() == 0) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return jdbcPool.poll();
    }

}
