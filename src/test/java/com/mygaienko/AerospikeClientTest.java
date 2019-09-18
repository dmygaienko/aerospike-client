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

    @Test
    public void testWriteUser() {
        Key key = new Key("test", "users", 2222);
        Bin bin1 = new Bin("username", "test user");
        Bin bin2 = new Bin("password", "test password");
        Bin bin3 = new Bin("gender", "test gender");
        Bin bin4 = new Bin("region", "test region");
        Bin bin5 = new Bin("lasttweeted", 0);
        Bin bin6 = new Bin("tweetcount", 0);
        client.put(new WritePolicy(), key, bin1, bin2, bin3, bin4, bin5, bin6);

        Record record = client.get(new Policy(), key);
        System.out.println(record);
    }

}
