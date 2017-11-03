/**
 * @author Alex
 */
package com.xyzdrivers.services;

import java.sql.*;
import java.util.*;

public class MembersService
{
//public methods
    /**
     * Fetches every row from Members table.
     * 
     * @param JDBC a <code>jdbcDriver</code> object to talk to the DB with a "MEMBERS" table.
     * 
     * @return List<Object[]> members. Each Object[] is a row of data.
     */
    //static is tmp until this file has more code.
    public static List<Object[]> getMembers(SQLService JDBC)
    {
        List<Object[]> members = null;
        
        try {
            members = JDBC.retrieve("MEMBERS");
        } catch (SQLException ex) {
            // HANDLE OR THROW EX ?
        }
        
        return members;
    }
}
