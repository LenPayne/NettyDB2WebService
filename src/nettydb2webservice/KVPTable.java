/*
 * Copyright 2014 Len Payne <len.payne@lambtoncollege.ca>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nettydb2webservice;

import com.ibm.as400.access.AS400JDBCDriver;
import java.sql.*;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/**
 * This project assumes you have a table called KVP with two VARCHAR columns
 * called ID and VAL. Note that the JDBC Driver only sends column names in
 * upper-case, so you must use upper-case column names on your DB.
 *
 * @author Len Payne <len.payne@lambtoncollege.ca>
 */
public class KVPTable {
    
    private Connection conn;
    private String jdbc;
    
    private HashMap<String, String> kvpMap;

    /**
     * The Constructor fills the map up with the contents of the table
     */
    public KVPTable() {
        // Load the AS400JDBCDriver into memory so that DriverManager finds it
        try {
            Class.forName("com.ibm.as400.access.AS400JDBCDriver");
        } catch (ClassNotFoundException ex) {
            System.err.println("JDBC Driver Not Found.");
        }

        // Set up the JDBC String 
        // (Replace Credentials.user and Credentials.pass with your username/password)
        String jdbcf = "jdbc:as400://%s;user=%s;password=%s;";
        jdbc = String.format(jdbcf, Credentials.url, Credentials.user, Credentials.pass);
        
        try {
            // Build the Connection
            conn = DriverManager.getConnection(jdbc);
            // Initialize the Empty HashMap
            kvpMap = new HashMap<>();

            // Run a SQL Statement to get the contents of the table
            String getAllSQL = String.format("SELECT * FROM \"%s\".\"%s\"",
                    Credentials.schema, Credentials.table);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(getAllSQL);

            // Put the table contents into a HashMap
            while (rs.next()) {
                kvpMap.put(rs.getString(1), rs.getString(2));
            }

            // Close the connection to finish the transaction
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(KVPTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Retrieves a value that is stored in the table
     *
     * @param key - The key
     * @return - The value
     */
    public String get(String key) {
        return kvpMap.get(key);
    }

    /**
     * Inserts or updates a value in the table
     *
     * @param key - The key
     * @param value - The value
     * @throws Exception - The DB write failed
     */
    public void put(String key, String value) throws Exception {
        if (dbPut(key, value)) {
            kvpMap.put(key, value);
        } else {
            throw new Exception("Database Write Failed");
        }
    }
    
    public void remove(String key) throws Exception {
        if (dbRemove(key)) {
            kvpMap.remove(key);
        } else {
            throw new Exception("Database Remove Failed");
        }
    }
    
    private boolean dbPut(String key, String value) {
        boolean worked = false;
        try {
            // Build the Connection
            conn = DriverManager.getConnection(jdbc);
            // Initialize the Empty HashMap
            kvpMap = new HashMap<>();

            // Run a SQL Statement to get the contents of the table   
            // DB2 Upsert Syntax: http://www-01.ibm.com/support/knowledgecenter/SSEPGG_9.1.0/com.ibm.db2.udb.admin.doc/doc/r0010873.htm
            String upsertSQL = String.format("MERGE INTO \"%s\".\"%s\" t1 USING "
                    + "(SELECT * FROM \"%s\".\"%s\") t2 ON (t1.id = t2.id) "
                    + "WHEN MATCHED THEN UPDATE SET val = ? "
                    + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (?, ?)",
                    Credentials.schema, Credentials.table, Credentials.schema, Credentials.table);
            PreparedStatement pstmt = conn.prepareStatement(upsertSQL);
            pstmt.setString(1, value);
            pstmt.setString(2, key);
            pstmt.setString(3, value);
            if (pstmt.executeUpdate() >= 0) {
                worked = true;
            }

            // Close the connection to finish the transaction
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(KVPTable.class.getName()).log(Level.SEVERE, null, ex);
        }
        return worked;
    }
    
    private boolean dbRemove(String key) {
        boolean worked = false;
        try {
            // Build the Connection
            conn = DriverManager.getConnection(jdbc);
            // Initialize the Empty HashMap
            kvpMap = new HashMap<>();

            // Run a SQL Statement to get the contents of the table   
            // DB2 Upsert Syntax: http://www-01.ibm.com/support/knowledgecenter/SSEPGG_9.1.0/com.ibm.db2.udb.admin.doc/doc/r0010873.htm
            String upsertSQL = String.format("DELETE FROM \"%s\".\"%s\" WHERE id = ?",
                    Credentials.schema, Credentials.table);
            PreparedStatement pstmt = conn.prepareStatement(upsertSQL);
            pstmt.setString(1, key);
            if (pstmt.executeUpdate() >= 0) {
                worked = true;
            }

            // Close the connection to finish the transaction
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(KVPTable.class.getName()).log(Level.SEVERE, null, ex);
        }
        return worked;
    }
    
    public JSONObject toJSON() {
        return new JSONObject(kvpMap);        
    }
    
    public JSONObject toJSON(String key) {
        JSONObject json = new JSONObject();
        if (kvpMap.containsKey(key)) {
            json.put(key, kvpMap.get(key));
        }
        return json;
    }
    
}
