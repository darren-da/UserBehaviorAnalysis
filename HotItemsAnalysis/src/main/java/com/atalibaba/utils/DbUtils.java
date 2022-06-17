package com.atalibaba.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author :YuFada
 * @date： 2020/9/6 0006 下午 21:57
 * Description：
 */
public class DbUtils {
    private static DruidDataSource dataSource;
    public static Connection getConnection() throws SQLException {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/testdb");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return  dataSource.getConnection();
    }
}