package basic;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * Created by caojiaqing on 24/05/2017.
 */
public class DataSourceUtil {
    private static Logger log = Logger.getLogger(DataSourceUtil.class);
    private static BasicDataSource ds = null;

    public Connection getConnection(){
        Connection con=null;
        try {
            if(ds!=null){
                con=ds.getConnection();
            }else{
                con=getDataSource().getConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    public BasicDataSource getDataSource() throws Exception{
        if(ds==null){
            Properties properties = new Properties();
            properties.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
            ds = new BasicDataSource();
            ds.setDriverClassName(properties.getProperty("jdbc.localSolution.driverClassName"));
            ds.setUrl(properties.getProperty("jdbc.localSolution.url"));
            ds.setUsername(properties.getProperty("jdbc.localSolution.username"));
            ds.setPassword(properties.getProperty("jdbc.localSolution.password"));
            ds.setMaxActive(200);//设置最大并发数
            ds.setInitialSize(30);//数据库初始化时，创建的连接个数
            ds.setMinIdle(50);//最小空闲连接数
            ds.setMaxIdle(200);//数据库最大连接数
            ds.setMaxWait(1000);
            ds.setMinEvictableIdleTimeMillis(60*1000);//空闲连接60秒中后释放
            ds.setTimeBetweenEvictionRunsMillis(5*60*1000);//5分钟检测一次是否有死掉的线程
            ds.setTestOnBorrow(true);
        }
        return ds;
    }

    /**
     * 释放数据源
     */
    public static void shutDownDataSource() throws Exception{
        if(ds!=null){
            ds.close();
        }
    }


    /**
     * 关闭连接
     */
    public static void closeCon(ResultSet rs, PreparedStatement ps, Connection con){
        if(rs!=null){
            try {
                rs.close();
            } catch (Exception e) {
                log.error("关闭结果集ResultSet异常！"+e.getMessage(), e);
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (Exception e) {
                log.error("预编译SQL语句对象PreparedStatement关闭异常！"+e.getMessage(), e);
            }
        }
        if(con!=null){
            try {
                con.close();
            } catch (Exception e) {
                log.error("关闭连接对象Connection异常！"+e.getMessage(), e);
            }
        }
    }



}
