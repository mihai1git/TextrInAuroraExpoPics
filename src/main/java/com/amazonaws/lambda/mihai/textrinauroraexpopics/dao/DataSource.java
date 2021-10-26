package com.amazonaws.lambda.mihai.textrinauroraexpopics.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.mysql.cj.jdbc.MysqlDataSource;

/**
 * contains different types of connections, types that have an inner Connection
 * @author Mihai ADAM
 *
 */
public class DataSource {

	//database link taken from AWS console
	protected static final String AURORA_URL;
	//protected static final String AURORA_URL = "jdbc:mysql://scans.c5ktggqvzn4c.us-east-2.rds.amazonaws.com:3306/expo_ml_scans";
	//protected static final String AURORA_URL = "jdbc:mysql://localhost:3306/expo_ml_scans";
	protected static final String AURORA_USER;
	protected static final String AURORA_PASS;
	
	static {
		AURORA_URL = System.getenv("AURORA_URL");
		AURORA_USER = System.getenv("AURORA_USER");
		AURORA_PASS = System.getenv("AURORA_PASS");
		
//		AURORA_URL = "jdbc:mysql://localhost:3306/expo_ml_scans";
//		AURORA_USER = "root";
//		AURORA_PASS = "";
	}
	
    /**
     * 
     * @return AURORA SQL DataSource that contains Connection
     */
    public Connection getConnection() {

        MysqlDataSource mysqlDs = null;
        Connection conn = null;
        try{
        	DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
            mysqlDs = new MysqlDataSource();
            mysqlDs.setURL(AURORA_URL);
            mysqlDs.setUser(AURORA_USER);
            mysqlDs.setPassword(AURORA_PASS);
            mysqlDs.setLoginTimeout(5);

            //mysqlDs.setConnectionLifecycleInterceptors("com.amazonaws.xray.sql.mysql.TracingInterceptor");
            //mysqlDs.setExceptionInterceptors("com.amazonaws.xray.sql.mysql.TracingInterceptor");
            
            conn = mysqlDs.getConnection();
        }
        catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }
    
    /**
     * 
     * @return AURORA SQL raw Connection
     */
    public Connection getConnectionUsingDM() {
        Connection conn = null;
        try{
        	Class.forName("software.aws.rds.jdbc.Driver");
            Properties mysqlConnectionProperties = new Properties();
            mysqlConnectionProperties.setProperty("user",AURORA_USER);
            mysqlConnectionProperties.setProperty("password",AURORA_PASS);
            
            DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
            conn = DriverManager.getConnection(AURORA_URL, mysqlConnectionProperties);

        }
        catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }
    
    /**
     * 
     * @return AURORA SQL Connection that uses Identity and Access Management service authentication mechanism
     */
    public Connection getConnectionUsingIAM() {

        Connection conn = null;
        try{
        	
            
            //conn = IAMAuroraConnector.getConnectionUsingIam();
        }
        catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return conn;
    }
    

}
