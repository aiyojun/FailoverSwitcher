package com.jpro;

import com.jpro.failover.IndexerPartition;
import com.jpro.failover.FailoverSwitcher;
import com.jpro.util.NumberUtil;
import org.junit.Test;

public class IndexerManagerTest {
    @Test
    public void run() {
        FailoverSwitcher manager = new FailoverSwitcher("/root/jqs/DataRestore/indexer_partitions", 100000);
//        List<Pair<Long, String>> res = manager.recover();
//        res.forEach(p -> System.out.println(String.format("-> %010d - %s", p.getKey(), p.getValue())));
//        System.exit(0);
        try {
            long ack = 19999;
            long count = 0;
            for (long idx = 0; idx < 200000; idx++) {
                if (idx > 29960) {
                    System.exit(2);
                }
                String data = String.format("rsa%015d,%s", idx + 1, NumberUtil.long2hex(idx + 1));

                manager.keep(idx + 1, data);
                if (idx > ack) {
                    manager.release(idx - ack);
                    count++;
                    if (count > 2000) {
                        manager.show();
                        System.exit(2);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void t2() {
        IndexerPartition indexer = IndexerPartition.reload("/root/jqs/DataRestore/indexer_partitions/indexer0000010000.lock",10000);
        indexer.recordState(1010);
//        System.out.println(indexer.hasData(10000));
//        System.out.println(indexer.getData(9998));
//        System.out.println(indexer.getData(9999));
//        System.out.println(indexer.getData(10000));
//        indexer.keep(10000, "rsa000000000020000,4e20");
//        System.out.println(indexer.getData(10000));
    }

    @Test
    public void t3() {
        IndexerPartition indexer = IndexerPartition.reload("/root/jqs/DataRestore/indexer_partitions/indexer0000020000.lock",10000);
//        System.out.println(indexer.getData());
//        indexer.recordState(2020);
    }

    @Test
    public void t4() {
        FailoverSwitcher manager = new FailoverSwitcher("/root/jqs/DataRestore/indexer_partitions", 100000,10000);
        for (long idx = 0; idx < 30000; idx++) {
            String data = String.format("rsa%015d,%s", idx + 1, NumberUtil.long2hex(idx + 1));
            manager.keep(idx + 1, data);
//            if (idx + 1 >= 10000) {
//                System.out.println("---stop");
//                System.exit(13);
//            }
        }
    }

    @Test
    public void t51() {
        IndexerPartition indexer = IndexerPartition.reload("/root/jqs/DataRestore/indexer_partitions/indexer0000020000.lock",10000);
        indexer.recover().forEach(p -> System.out.println(p.getKey() + " ### " + p.getValue()));
//        System.out.println("2001" + indexer.hasData(2001));
//        System.out.println("2001" + indexer.getData(2001));
//        System.out.println("2002" + indexer.hasData(2002));
//        System.out.println("2002" + indexer.getData(2002));
    }

    @Test
    public void t5() {
//        show("/root/jqs/DataRestore/indexer_partitions/indexer0000000000.lock", 0);
//        show("/root/jqs/DataRestore/indexer_partitions/indexer0000010000.lock", 1);
        show("/root/jqs/DataRestore/indexer_partitions/indexer0000020000.lock", 2);
//        show("/root/jqs/DataRestore/indexer_partitions/indexer0000030000.lock", 3);
    }

    public static void show(String path, long id) {
        IndexerPartition.reload(path,10000).recordState(id);
    }
}
