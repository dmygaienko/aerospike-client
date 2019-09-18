package com.mygaienko;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AerospikeClientTest {

    private AerospikeClient client;

    @Before
    public void setUp() {
        client = new AerospikeClient("localhost", 3000);
    }

    @After
    public void close() {
        client.close();
    }

    @Test
    public void testWrite() {
        Key key = new Key("test", "demo", 111);
        Bin int_bin = new Bin("intbin", 777);
        Bin str_bin = new Bin("strbin", "test string");
        client.put(new WritePolicy(), key, int_bin, str_bin);
    }

    @Test
    public void testRead() {
        Key key = new Key("test", "demo", 111);
        Record record = client.get(new Policy(), key);
        System.out.println(record);
    }

}
