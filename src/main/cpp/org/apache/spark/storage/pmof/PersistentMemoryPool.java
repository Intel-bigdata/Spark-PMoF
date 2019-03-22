/* 
 * Copyright (C) 2018 Intel Corporation
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * 
 */
package org.apache.spark.storage.pmof;
import java.util.Arrays;
import java.util.concurrent.*;
import org.apache.commons.cli.*;
import sun.misc.Unsafe;
import static java.lang.Math.toIntExact;

class ArgParser {

    CommandLine cmd;
    Options options = new Options();
    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    ArgParser (String[] args) {

        Option device = new Option("d", "device", true, "pmem device path");
        device.setRequired(true);
        options.addOption(device);

        Option runtime = new Option("r", "runtime", true, "total run time");
        runtime.setRequired(true);
        options.addOption(runtime);

        Option thread_num = new Option("t", "thread_num", true, "parallel threads number");
        thread_num.setRequired(true);
        options.addOption(thread_num);

        Option block_size = new Option("bs", "block_size", true, "block size for each request(KB)");
        block_size.setRequired(true);
        options.addOption(block_size);

        try {
            this.cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

    }

    public String get(String key) {
	    String ret = "";
        ret = this.cmd.getOptionValue(key);
	    return ret;
    }
}

class Monitor {
    long committedBytes = 0;
    long submittedBytes = 0;
    boolean alive = true;
    int bs;
    ExecutorService monitor_thread;
	Monitor (int bs) {
        this.bs = bs;
	this.monitor_thread = Executors.newFixedThreadPool(1);
        this.monitor_thread.submit(this::run);
	}

	void run () {
        long last_committed_bytes = 0;
	int elapse_sec = 0;
        while(alive) {
            System.out.println("Second " + elapse_sec + "(MB/s): " + (this.committedBytes - last_committed_bytes) / 1024 / 1024);
	    last_committed_bytes = this.committedBytes;
	    elapse_sec += 1;
	    try {
	        Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.exit(1);
	    }
	}
    }

    synchronized void incCommittedJobs(long size) {
        this.committedBytes += size;
        this.submittedBytes -= size;
    }

    synchronized void incSubmittedJobs(long size) {
        this.submittedBytes += size;
    }

    void stop() {
        this.alive = false;
        /*while(this.submittedJobs > 0) {
            System.out.println("still remain inflight io: " + this.submittedJobs);
  	    try {
	          Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.exit(1);
	          }
        }
        System.out.println("inflight io: " + this.submittedJobs);
        */
        this.monitor_thread.shutdown();
    }

    boolean alive() {
        return this.alive;
    }
}

public class PersistentMemoryPool{
    static {
        System.loadLibrary("jnipmdk");
    }
        private static native long nativeOpenDevice(String path, int maxStage, int maxMap, long size);
        private static native int nativeSetMapPartition(long deviceHandler, int numPartitions, int stageId, int mapId, int partitionId, long size, byte[] data, boolean set_clean);
        private static native int nativeSetReducePartition(long deviceHandler, int numPartitions, int stageId, int partutionId, long size, byte[] data, boolean set_clean);
        private static native byte[] nativeGetMapPartition(long deviceHandler, int stageId, int mapId, int partutionId);
        private static native long[] nativeGetMapPartitionBlockInfo(long deviceHandler, int stageId, int mapId, int partitionId);
        private static native int nativeCloseDevice(long deviceHandler);

    String device;
    int thread_num;
    byte[] bytes;
    ExecutorService executor;
    long writerHandler;
    boolean alive = true;
    int bs;
    Monitor monitor;
    int block_size;


    PersistentMemoryPool() {
    }

    class Writer implements Callable<Integer> {

      int i = 0, j = 0;
      int i_multi = 0;
      int k;
      Unsafe UNSAFE;

      Writer(int k_arg) {
        k = k_arg;

        try {
          java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          UNSAFE = (Unsafe)f.get(null);
        } catch (Exception e) {
          throw new RuntimeException("Unable to initialize UNSAFE.");
        }
      }

      @Override
      public Integer call() throws Exception {
        //System.out.println("Enter write thread");
        System.out.println("Start set partition "+k);
	      while (monitor.alive() == true) {
            if (i_multi == 100) {
              i_multi = 0;
              i++;
            }
            if (i > 9999) {
                i = 0;
                j++;
            }
            if (j > 9999) {
                break;
            }
            PersistentMemoryPool.this.monitor.incSubmittedJobs(PersistentMemoryPool.this.block_size);
            nativeSetMapPartition(PersistentMemoryPool.this.writerHandler, 10000, k, j, i, PersistentMemoryPool.this.block_size, PersistentMemoryPool.this.bytes, false);
            i_multi += 1;
            PersistentMemoryPool.this.monitor.incCommittedJobs(PersistentMemoryPool.this.block_size);
 
            /*long[] block_info = nativeGetMapPartitionBlockInfo(PersistentMemoryPool.this.writerHandler, k, j, i);
            //System.out.println("Start to get partition block info for "+k+"_"+j+"_"+i+", numBlocks: "+block_info.length/2);
            int index = 0;
            while (index+1 < block_info.length) {
              PersistentMemoryPool.this.monitor.incSubmittedJobs(block_info[index+1]);
              byte[] data = new byte[toIntExact(block_info[index+1])];
              UNSAFE.copyMemory(null, block_info[index], data, Unsafe.ARRAY_BYTE_BASE_OFFSET, block_info[index+1]);
              PersistentMemoryPool.this.monitor.incCommittedJobs(block_info[index+1]);
              i_multi += 1;
              index += 2;
            }*/
        }
        return 0;
      }
    }

    public void run (Monitor monitor, String dev, int block_size, byte[] data, int threads){
        this.monitor = monitor;
        this.block_size = block_size;
        this.bs = block_size / 1024;
        int bs_mb = bs / 1024;
        this.bytes = data;
        this.thread_num = threads;
        //long size = 107374182400L;
        long size = 0;

        this.writerHandler = nativeOpenDevice(dev, 50, 10, size);
        System.out.println("Thread Num: " + this.thread_num + ", block_size: " + bs + "KB, Device: " + dev);
        
	      this.executor = Executors.newFixedThreadPool(this.thread_num);
        for (int k = 0; k < this.thread_num; k++) {
            this.executor.submit(new Writer(k));
        }
	    
    }

    public void stop() {
        alive = false;
        System.out.println("start to close writer thread.");
        System.out.println("inflight bytes: " + monitor.submittedBytes);
        this.executor.shutdown();
        try {
        this.executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        System.out.println("inflight bytes: " + monitor.submittedBytes);
        this.executor.shutdown();
        System.out.println("closed writer thread, start to close device.");
        nativeCloseDevice(writerHandler);
    }

    public static void main(String[] args) {

	ArgParser arg_parser = new ArgParser(args);
	String[] device_list = arg_parser.get("device").trim().split("\\s*,\\s*", -1);
        int runtime = Integer.parseInt(arg_parser.get("runtime"));
        int thread_num = Integer.parseInt(arg_parser.get("thread_num"));
        int bs = Integer.parseInt(arg_parser.get("block_size"));
        int block_size = bs * 1024;

        byte data[] = new byte[block_size];
        Arrays.fill(data,(byte)'a');
    
    	PersistentMemoryPool[] writer = new PersistentMemoryPool[device_list.length];
    	Monitor monitor = new Monitor(bs);
    	for (int i = 0; i < device_list.length; i++) { 
            writer[i] = new PersistentMemoryPool();
            writer[i].run(monitor, device_list[i], block_size, data, thread_num);
    	}

        try{
            Thread.sleep(1000 * runtime);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        monitor.stop();
    	for (int i = 0; i < device_list.length; i++) { 
    	    writer[i].stop();
    	}
      System.out.println("inflight bytes: " + monitor.submittedBytes);
    }
}
