package com.mygaienko;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class AerospikeClientPerformanceTest {

    private AerospikeClient client;

    private static final int FROM = 1000;
    private static final int END_EXCLUSIVE = 50000;

    @Before
    public void setUp() {
        client = new AerospikeClient("localhost", 3000);
        createUsers();
    }

    @After
    public void close() {
        deleteUsers();
        deleteIndex();
        client.close();
    }

    private void deleteIndex() {
        client.dropIndex(null, "test", "users",
                "region_index");
    }

    private void deleteUsers() {
        IntStream.range(FROM, END_EXCLUSIVE)
                .forEach(this::deleteUser);
    }

    private void deleteUser(int i) {
//        System.out.println("Deleting user: " + i);
        client.delete(new WritePolicy(), new Key("test", "users", i));
    }

    @Test
    public void testWriteUser() {
        StopWatch watch = new StopWatch();
        watch.start();

        RecordSet record = findRecord();

        watch.stop();
        System.out.println("Time to get " + watch.getTime(TimeUnit.MILLISECONDS) + "\n" + record);
    }

    @Test
    public void testWriteUserAndCreateIndex() {
        IndexTask task = client.createIndex(null, "test", "users",
                "region_index", "region", IndexType.STRING);
        task.waitTillComplete(100);

        StopWatch watch = new StopWatch();
        watch.start();

        RecordSet record = findRecord();

        watch.stop();
        System.out.println("Time to get " + watch.getTime(TimeUnit.MILLISECONDS) + "\n" + record);
    }

    private RecordSet findRecord() {
        Statement stmt = new Statement();
        stmt.setNamespace("test");
        stmt.setSetName("users");
        stmt.setIndexName("region_index");
        stmt.setBinNames("region");
        stmt.setFilters(Filter.equal("region", "test region2000"));
        return client.query(null, stmt);
    }

    private void createUsers() {
        IntStream.range(FROM, END_EXCLUSIVE)
                .forEach(this::createUser);
    }

    private void createUser(int i) {
//        System.out.println("Creating user: " + i);
        Key key = new Key("test", "users", i);
        Bin bin1 = new Bin("username", "test user" + i);
        Bin bin2 = new Bin("password", "test password" + i);
        Bin bin3 = new Bin("gender", "test gender" + i);
        Bin bin4 = new Bin("region", "test region" + i);
        Bin bin5 = new Bin("lasttweeted", 0);
        Bin bin6 = new Bin("tweetcount", 0);
        client.put(new WritePolicy(), key, bin1, bin2, bin3, bin4, bin5, bin6);
    }

}
