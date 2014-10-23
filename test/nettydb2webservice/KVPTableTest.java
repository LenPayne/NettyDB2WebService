/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nettydb2webservice;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author c0587637
 */
public class KVPTableTest {

    public KVPTableTest() {
    }

    /**
     * Test of put method, of class KVPTable.
     */
    @Test
    public void testPutGet() throws Exception {
        System.out.println("put-get");
        String key = "abc";
        String value = "abc";
        KVPTable instance = new KVPTable();
        instance.put(key, value);
        assertEquals(instance.get(key), value);
    }

    /**
     * Test of remove method, of class KVPTable.
     */
    @Test
    public void testPutRemove() throws Exception {
        System.out.println("remove");
        String key = "abc";
        String value = "abc";
        KVPTable instance = new KVPTable();
        instance.put(key, value);
        instance.remove(key);
        assertEquals(instance.get(key), null);        
    }

}
