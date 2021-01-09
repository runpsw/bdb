package com.enjoy;

import com.enjoy.commons.JdbcPool;
import com.enjoy.conf.ConfManager;
import com.enjoy.constants.Constants;
import com.enjoy.entity.PointEntity;
import com.sleepycat.je.*;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sven
 * @date 2021-01-09
 *
 * this class is for data ingest
 */
public class Apps {

    private static final Logger logger = LoggerFactory.getLogger(Apps.class);

    // 初始化bdb
    public Database getOrCreateBdb(String dbName){
        logger.info("start init berkeley db");

        File dbPath = new File(ConfManager.getStrValue(Constants.BDB_PATH));
        if (!dbPath.exists()){
            dbPath.mkdir();
        }
        EnvironmentConfig dbEnvConf = new EnvironmentConfig();
        dbEnvConf.setAllowCreate(true);
        dbEnvConf.setTransactional(true);
        dbEnvConf.setCacheSize(1024 * 1024 * 20);
        Environment env = new Environment(dbPath,dbEnvConf);

        DatabaseConfig dbConf = new DatabaseConfig();
        dbConf.setAllowCreate(true);
        dbConf.setTransactional(true);

        Database db = env.openDatabase(null,dbName,dbConf);
        logger.info("berkeley db is inited in the path" + ConfManager.getStrValue(Constants.BDB_PATH));
        return db;
    }

    // 往bdb中读取数据
    public String getOffset(String dbName) {

        String data = null;
        Database db = getOrCreateBdb(dbName);
        DatabaseEntry key = new DatabaseEntry(dbName.getBytes(Charsets.UTF_8));
        DatabaseEntry valueData = new DatabaseEntry();

        OperationStatus st = db.get(null, key, valueData, LockMode.DEFAULT);
        logger.info(st.toString());
        if (st.equals(OperationStatus.SUCCESS)) {
            byte[] dataBytes = valueData.getData();
            data = new String(dataBytes,Charsets.UTF_8);
        }
        return data;
    }

    // 往bdb中更新数据
    public void updateOffset(String newOffset,String dbName) {
        Database db = getOrCreateBdb(dbName);
        DatabaseEntry key = new DatabaseEntry(dbName.getBytes(Charsets.UTF_8));
        DatabaseEntry value = new DatabaseEntry(newOffset.getBytes(Charsets.UTF_8));
        OperationStatus put = db.put(null, key, value);
        logger.info(put.toString());
    }

    // 读取mysql2w条数据
    public List<PointEntity> read2w(String tb) {

        List<PointEntity> points = new ArrayList<PointEntity>();

        JdbcPool jdbcPool = JdbcPool.getInstance();
        Connection conn = jdbcPool.getConn();
        Statement stmt = null;

        // 获取起始offset


        // 根据起始offset 拼接成可执行的sql
        String sql = "";

        try {
            stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);

            while (resultSet.next()){
                // 将查询结果封装到list<point>中
                PointEntity point = new PointEntity();
                point.setName(resultSet.getString(1));
                point.setDesc(resultSet.getString(2));
                point.setTs(resultSet.getString(3));
                point.setValue(resultSet.getString(4));
                points.add(point);
            }


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        // 读完数据后这里更新一下offset


        return points;
    }

    /*
    / 往本地文件中写入xx条数据
    首先获取写数据的offset
    然后写入数据
    写入数据后更新写入数据的offset

    数据一致性的保证
    如果如数据的offset - 写数据的offset 不等于batch 值的话。那么就要确认数据是否出现过问题

    使用另外一张表来记录程序是否中断过，中断过就要要终端处理的机制

     */


    // 写完就更新offset

    // 根据新offset接着读数据

    public static void main(String[] args) {
        Apps apps = new Apps();

//        Database test = apps.getOrCreateBdb("test");
//        DatabaseEntry key = new DatabaseEntry("test".getBytes(Charsets.UTF_8));
//        DatabaseEntry value = new DatabaseEntry("1".getBytes(Charsets.UTF_8));
//
//        OperationStatus put = test.put(null, key, value);
//        if (put == OperationStatus.SUCCESS) {
//            logger.info("offset has been updated");
//        }

//        System.out.println(apps.getOffset("test"));

//        System.out.println("122".matches("^\\d+$"));

    }
}
