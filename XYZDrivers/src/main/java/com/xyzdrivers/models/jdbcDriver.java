/**
 * @file    jdbcDriver.java
 * @author  alexander collins
 * @created 02/11/2017
 * @notes   - Feel free to fix any stupid mistakes
 *          - The private functions could probably be made public.
 *          - Haven't tested it since creating it
 * @issues  1. getPrimaryKeyName() doesn't work, any function using it won't work.
 */
package com.xyzdrivers.models;

import java.sql.*;
import java.util.*;

public class jdbcDriver
{
//variables
    private Connection DB;
    private DatabaseMetaData DBMetaData;
    
    private PreparedStatement statement;
    private ResultSetMetaData resultsMetaData;
    private ResultSet results;
//constructors
    /**
     * Constructor for jdbcDriver.
     * <code>dbConnection</code> is tested by retrieving <code>DatabaseMetaData</code> from the <code>Connection</code>.
     * 
     * @param dbConnection A <code>Connection</code> to the DB being accessed.
     * 
     * @throws SQLException 
     * @throws java.lang.ClassNotFoundException 
     */
    public jdbcDriver(Connection dbConnection)
            throws SQLException
    {
        //pass DB connection
        DB = dbConnection;
        
        //get database metadata
        DBMetaData = DB.getMetaData();
    }
    
    /**
     * Constructor for jdbcDriver.
     * Attempts to create a <code>Connection</code> with the passed <code>URL</code>, <code>USER</code> and <code>PASS</code>.
     * 
     * @param URL URL of the DB to create a Connection to.
     * 
     * @throws SQLException 
     * @throws java.lang.ClassNotFoundException 
     */
    public jdbcDriver(String URL)
            throws SQLException, ClassNotFoundException
    {
        //connect to database
        Class.forName("com.mysql.jdbc.Driver");
        DB = DriverManager.getConnection(URL);
        
        //get database metadata
        DBMetaData = DB.getMetaData();
    }
    
    /**
     * Constructor for jdbcDriver.
     * Attempts to create a <code>Connection</code> with the passed <code>URL</code>, <code>USER</code> and <code>PASS</code>.
     * 
     * @param URL URL of the DB to create a Connection to.
     * @param USER Username to use when creating a Connection to <code>URL</code>.
     * @param PASS Password of the <code>USER</code>.
     * 
     * @throws SQLException 
     * @throws java.lang.ClassNotFoundException 
     */
    public jdbcDriver(String URL, String USER, String PASS)
            throws SQLException, ClassNotFoundException
    {
        //connect to database
        Class.forName("com.mysql.jdbc.Driver");
        DB = DriverManager.getConnection(URL, USER, PASS);
        
        //get database metadata
        DBMetaData = DB.getMetaData();
    }
    
//public methods
    private void close() throws SQLException
    {
        results.close();
        statement.close();
        DB.close();
    }
    private List<String> getAllTableNames() throws SQLException
    {
        List<String> tableNames = new ArrayList();
        
        results = DBMetaData.getTables(null, null, "%", null);
        while(results.next())
            if (!results.getString(3).contains("SYS"))
                tableNames.add(results.getString(3));
            
        System.out.println(tableNames.toString());
        return tableNames;
    }
    private List<String> getAllColumnNames(String tableName) throws SQLException
    {
        List<String> columnNames = new ArrayList();
        
        statement = DB.prepareStatement("SELECT * FROM "+tableName+" FETCH FIRST 1 ROWS ONLY");
        results = statement.executeQuery();
        
        //results = statement.executeQuery("SELECT * FROM "+tableName);
        
        resultsMetaData = results.getMetaData();
        for (int i = 1; i < resultsMetaData.getColumnCount(); i++)
            columnNames.add(resultsMetaData.getColumnName(i));
        
        return columnNames;
    }
    private String getPrimaryKeyName(String table) throws SQLException
    {
        results = DBMetaData.getPrimaryKeys("", "", table);
        
        if (!results.next())
            throw new IllegalArgumentException("Could not find PRIMARY KEY in "+table+". Please specify the primaryKeyColumn.");
        
        return results.getString("PK_NAME");
    }
//exists
    /**
     * Checks if <code>table</code> exists.
     * 
     * @param table The name of the table to look for.
     * 
     * @return The boolean result of <code>(query results).next()</code>
     * 
     * @throws SQLException 
     */
    public boolean exists(String table)
            throws SQLException
    {
        //prepare statement
        statement = DB.prepareStatement("SELECT * FROM "+table);
        //execute statement
        results = statement.executeQuery();
        //return results
        return results.next();
    }
        
    /**
     * Checks if <code>column</code> exists in <code>table</code>.
     * 
     * @param table The name of the table containing the data to retrieve data from
     * @param column The name of the column to look for in <code>table</code>
     * 
     * @return The boolean result of <code>(query results).next()</code>
     * 
     * @throws SQLException 
     */
    public boolean exists(String table, String column)
            throws SQLException
    {
        //prepare statement
        statement = DB.prepareStatement("SELECT "+column+" FROM "+table+" WHERE "+column+" = ?");
        //execute statement
        results = statement.executeQuery();
        //return results
        return results.next();
    }
    
    /**
     * Checks if <code>query</code> exists in <code>table->column</code>.
     * 
     * @param table The name of the table containing the item
     * @param column The name of the column containing the item to retrieve from table
     * @param item The item to be look for in <code>table->column</code>
     * 
     * @return The boolean result of <code>(query results).next()</code>
     * 
     * @throws SQLException 
     */
    public boolean exists(String table, String column, Object item)
            throws SQLException
    {
        //prepare statement
        statement = DB.prepareStatement("SELECT "+column+" FROM "+table+" WHERE "+column+" = ?");
        statement.setObject(1, item);
        //execute statement
        results = statement.executeQuery();
        //return results
        return results.next();
    }
//retrieve
    /**
     * 
     * @param table
     * @return
     * @throws SQLException 
     */
    public List<Object[]> retrieve(String table)
            throws SQLException
    {
        List<Object[]> data;
        Object[] column;
        
        //check table and table->column exist
        if (!exists(table))
            throw new IllegalArgumentException();
        //prepare statement
        statement = DB.prepareStatement("SELECT * FROM "+table);
        //execute statement
        results = statement.executeQuery();
        resultsMetaData = results.getMetaData();
        //add results to data
        int columnCount = resultsMetaData.getColumnCount();
        data = new ArrayList<>();
        for (int row = 0; results.next(); row++)
        {   
            column = new Object[columnCount];
            for (int col = 1; col < columnCount; col++)
                column[col-1] = results.getObject(col);
            
            data.add(column);
        }
        //return results
        return data;
    }
    /**
     * Retrieve a <code>Collection</code> of all items in <code>table->column</code>
     * 
     * @param table the table containing the item be to retrieved
     * @param column the column containing the item to be retrieved from table
     * 
     * @return an Object containing the item found
     * 
     * @throws SQLException 
     */
    public List<Object> retrieve(String table, String column)
            throws SQLException
    {
        List<Object> data;
        
        //check table and table->column exist
        if (!exists(table, column))
            throw new IllegalArgumentException();
        //prepare statement
        statement = DB.prepareStatement("SELECT "+column+" FROM "+table);
        //execute statement
        results = statement.executeQuery();
        //add results to data
        data = new ArrayList();
        for (int i = 0; results.next(); i++)
            data.add(results.getObject(column));
        //return results
        return data;
    }
    /**
     * Retrieve an item from <code>table->column</code>, where the PRIMARY KEY
     * is <code>primaryKey</code>.
     * 
     * @param table the table containing the item be to retrieved
     * @param column the column containing the item to be retrieved from table
     * @param primaryKey the PRIMARY KEY of the item to be retrieved
     * 
     * @return an Object containing the item found
     * 
     * @throws IllegalArgumentException if <code>(query results).next()</code> returns false.
     * @throws SQLException 
     */
    public Object retrieve(String table, String column, Object primaryKey)
            throws SQLException, IllegalArgumentException
    {
        //prepare statement
        statement = DB.prepareStatement("SELECT "+column+" FROM "+table+" WHERE "+getPrimaryKeyName(table)+" = ?");
        statement.setObject(1, primaryKey);
        //execute statement
        results = statement.executeQuery();
        
        //return results
        if (results.next())
            return results.getObject(column);
        else
            throw new IllegalArgumentException();
    }
    /**
     * Retrieve an item from <code>table->column</code>, where the PRIMARY KEY
     * is <code>primaryKey</code>.
     * 
     * @param table the table containing the item be to retrieved
     * @param column the column containing the item to be retrieved from table
     * @param primaryKeyColumn the name of the PRIMARY KEY column
     * @param primaryKey the PRIMARY KEY of the item to be retrieved
     * 
     * @return an Object containing the item found
     * 
     * @throws IllegalArgumentException if <code>(query results).next()</code> returns false.
     * @throws SQLException 
     */
    public Object retrieve(String table, String column, String primaryKeyColumn, Object primaryKey)
            throws SQLException, IllegalArgumentException
    {
        //prepare statement
        statement = DB.prepareStatement("SELECT "+column+" FROM "+table+" WHERE "+primaryKeyColumn+" = ?");
        statement.setObject(1, primaryKey);
        //execute statement
        results = statement.executeQuery();
        
        //return results
        if (results.next())
            return results.getObject(column);
        else
            throw new IllegalArgumentException();
    } 
//update
    /**
     * Update an item from <code>table->column</code>, where the PRIMARY KEY
     * is <code>primaryKey</code>.
     * 
     * @param table the table containing the item be to retrieved
     * @param column the column containing the item to be retrieved from table
     * @param primaryKey the PRIMARY KEY of the item to be retrieved
     * @param value the value to update the found item with
     * 
     * @throws SQLException 
     */
    public void update(String table, String column, Object primaryKey, Object value)
            throws SQLException
    {
        //prepare statement
        statement = DB.prepareStatement("UPDATE "+table+" SET "+column+" = ? WHERE "+getPrimaryKeyName(table)+" = ?");
        statement.setObject(1, value);
        statement.setObject(2, primaryKey);
        //execute statement
        statement.executeUpdate();
    }
    /**
     * Update an item from <code>table->column</code>, where the PRIMARY KEY
     * is <code>primaryKey</code>.
     * 
     * @param table the table containing the item be to retrieved
     * @param column the column containing the item to be retrieved from table
     * @param primaryKeyColumn the name of the PRIMARY KEY column
     * @param primaryKey the PRIMARY KEY of the item to be retrieved
     * @param value the value to update the found item with
     * 
     * @throws SQLException 
     */
    public void update(String table, String column, String primaryKeyColumn, Object primaryKey, Object value)
            throws SQLException
    {
        //prepare statement
        statement = DB.prepareStatement("UPDATE "+table+" SET "+column+" = ? WHERE "+primaryKeyColumn+" = ?");
        statement.setObject(1, value);
        statement.setObject(2, primaryKey);
        //execute statement
        statement.executeUpdate();
    }
//insert
    /**
     * Insert a new row of <code>values</code> into <code>table</code>.
     * The array of values are inserted in the order they are found (index 0 to 
     * index values.length).
     * 
     * @param table the table you want to insert a row into
     * @param values an array containing the values to insert
     * 
     * @throws SQLException 
     */
    public void insert(String table, Object[] values) throws SQLException
    {
        //prepare statement query "INSERT INTO table VALUES (?, ...)"
        String insertQuery = "INSERT INTO "+table+" VALUES (";
        for (int i = 0; i < values.length; i++)
        {
            if (i == values.length-1)
                insertQuery += ("?)");
            else
                insertQuery += ("?, ");
        }
        //prepare statement
        statement = DB.prepareStatement(insertQuery);
        for (int i = 0; i < values.length; i++)
            statement.setObject(i+1, values[i]);
        //execute statement
        statement.executeUpdate();
    }
    /**
     * Insert a set of <code>values</code> into <code>table</code>.
     * The array of values are inserted in the order they are found (index 0 to 
     * index values.length).
     * 
     * @param table the table you want to insert a row into
     * @param values an array containing array(s) of values to be inserted
     * 
     * @throws SQLException 
     */
    public void insert(String table, Object[][] values) throws SQLException
    {
        for (int j = 0; j < values.length; j++)
        {
            //prepare statement query "INSERT INTO table VALUES (?, ...)"
            String insertQuery = "INSERT INTO "+table+" VALUES (";
            for (int i = 0; i < values[j].length; i++)
            {
                if (i == values[j].length-1)
                    insertQuery += ("?)");
                else
                    insertQuery += ("?, ");
            }
            //prepare statement
            statement = DB.prepareStatement(insertQuery);
            for (int i = 0; i < values[j].length; i++)
                statement.setObject(i+1, values[j][i]);
            //execute statement
            statement.executeUpdate();
        }
    }
//remove
    /**
     * Remove a row of data from <code>table</code> where <code>query</code> is
     * true. <code>query</code> must be valid SQL syntax, the <code>query</code>
     * String is appended to the end of "DELETE FROM "+table+" WHERE ".
     * 
     * @param table the table you want to remove the row from
     * @param query a string containing an SQL removal condition, appended to 
     * the end of "DELETE FROM "+table+" WHERE ".
     * 
     * @throws SQLException 
     */
    public void remove(String table, String query) throws SQLException
    {
        statement = DB.prepareStatement("DELETE FROM "+table+" WHERE "+query);
        statement.executeUpdate();
    }
}
