package com.mygaienko;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Test;

public class AerospikeClientTest {

    @Test
    public void testWrite() {
        AerospikeClient client = new AerospikeClient("localhost", 3000);

        Key key = new Key("test", "demo", 111);
        Bin int_bin = new Bin("intbin", 777);
        Bin str_bin = new Bin("strbin", "test string");
        client.put(new WritePolicy(), key, int_bin, str_bin);
    }

    @Test
    public void testRead() {
        AerospikeClient client = new AerospikeClient("localhost", 3000);

        Key key = new Key("test", "demo", 111);
        Record record = client.get(new Policy(), key);
        System.out.println(record);
    }

}
