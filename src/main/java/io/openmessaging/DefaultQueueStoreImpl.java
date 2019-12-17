package io.openmessaging;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {


    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

    //以块为索引
    public Map<String, List<Block>> blockMap = new ConcurrentHashMap<>();

    public Map<String, DataCache> cacheMap = new ConcurrentHashMap<>();

    //父目录路径
    public static final String DIRPATH = "./data/";

    Lock lock = new ReentrantLock();//可重入锁

    //预先创建根目录
    static {
        File file = new File(DIRPATH);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void put(String queueName, byte[] message) {
        //获得在哪个文件中
        int hash = hashFile(queueName);
        //为每个文件新建一个地址，一共16个文件
        String path = DIRPATH + hash + ".txt";
        //上锁
        lock.lock();
        //创建文件
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        //如果该队列没有在blockMap中就将其
        if (!blockMap.containsKey(queueName)) {
            List<Block> list = new ArrayList();
            blockMap.put(queueName, list);
        }

        //如果该队列没在缓冲map中
        if (!cacheMap.containsKey(queueName)) {
            DataCache dataCache = new DataCache();
            cacheMap.put(queueName, dataCache);
        }

        //获取该队列的缓存文件
        DataCache dataCache = cacheMap.get(queueName);
        //当缓存的数量达到10的时候
        if (dataCache.count == 10) {
            //将数据写入
            FileChannel fileChannel = null;
            // long fileLength = 0;
            try {
                fileChannel = new RandomAccessFile(file, "rw").getChannel();
                //fileLength = raf.length();
            } catch (Exception e) {
                e.printStackTrace();
            }

            long blockPosition;
            try {
                //FileChannel实例的size()方法将返回该实例所关联文件的大小，为什么要调用两次getLeastBlockPosition函数
                //这里可能是作者测试没有通过，可能是因为缓存的大小超过了1024
                blockPosition = getLeastBlockPosition(getLeastBlockPosition(fileChannel.size()));
                //position()方法获取FileChannel的当前位置
                Block block = new Block(blockPosition, dataCache.dataBuffer.position());
                block.size = 10;
                blockMap.get(queueName).add(block);
                dataCache.dataBuffer.flip();
                fileChannel.position(blockPosition);
                fileChannel.write(dataCache.dataBuffer);
                dataCache.dataBuffer.clear();

            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    fileChannel.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        } else {
            dataCache.dataBuffer.putInt(message.length);
            dataCache.dataBuffer.put(message);
            dataCache.count++;
        }


//        MappedByteBuffer buffer = null;
//        long blockPosition = 0;
//        try {
//            blockPosition = getLeastBlockPosition(getLeastBlockPosition(fileChannel.size()));
//            // System.out.println(blockPosition);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        //如果还没有创建Block
//        if (blockMap.get(queueName).size() == 0) {
//            Block block = new Block(blockPosition, message.length + 4);
//            block.size++;
//            blockMap.get(queueName).add(block);
//            try {
//                buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, blockPosition, 4 + message.length);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        } else {
//            //如果已经存在block,则放在blcok里面,如果满了,就另外放
//            Block lastBlock = blockMap.get(queueName).get(blockMap.get(queueName).size() - 1);
//
//
//            if (lastBlock.length + message.length + 4 <= 2048) {
//
//                try {
//                    buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, lastBlock.startPosition+lastBlock.length, message.length + 4);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                lastBlock.length = lastBlock.length + message.length + 4;
//                lastBlock.size++;
//            } else {
//                Block block = new Block(blockPosition, message.length + 4);
//                blockMap.get(queueName).add(block);
//                try {
//                    buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, blockPosition, message.length + 4);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                block.size++;
//            }
//        }
//        buffer.putInt(message.length);
//        buffer.put(message);
//        try {
//            fileChannel.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        lock.unlock();


    }


    //offset和num的意义是什么？
    public Collection<byte[]> get(String queueName, long offset, long num) {

        //队列不存在
        if (!blockMap.containsKey(queueName)) {
            return EMPTY;
        }
        //消息集合
        List<byte[]> msgs = new ArrayList();
        //blocks是所有的的block的列表
        List<Block> blocks = blockMap.get(queueName);

        //找到存放改队列数据的文件
        int hash = hashFile(queueName);
        String path = DIRPATH + hash + ".txt";
        FileChannel fileChannel = null;
        //用于寻找block的位置
        int size = blocks.get(0).size;
        int eleNum = 0;
        //记录了目标block所在的下标
        int blockNum = 0;
        lock.lock();
        try {
            //新建读取的filechannel，block的size一般都是10
            fileChannel = new RandomAccessFile(new File(path), "rw").getChannel();
            for (int i = 1; i < blocks.size() && size < offset; i++, blockNum++) {
                size += blocks.get(i).size;
            }
            //向后退回一个就是要找的block及其之前的size之和
            size = size - blocks.get(blockNum).size;

            for (int i = blockNum; i < blocks.size(); i++) {
                //size+=blocks.get(i).size;
                // size-=blocks.get(i).size;
                int length = blocks.get(i).length;
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, blocks.get(i).startPosition, length);
                int sum = 0;
                //寻找精确的位置，因为一个block里面包含至多十个消息
                while (sum < length && size < offset) {
                    int len = buffer.getInt();
                    sum += 4;
                    sum += len;
                    buffer.position(sum);
                    size++;
                }

                if (size >= offset) {
                    while (buffer.position() < length && eleNum <= num) {
                        int len = buffer.getInt();
                        byte[] temp = new byte[len];
                        buffer.get(temp, 0, len);
                        eleNum++;
                        msgs.add(temp);
                    }
                    if (eleNum > num) {
                        break;
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                fileChannel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            lock.unlock();

        }

        return msgs;
    }


    //根据队列的名字hash到对应的文件中,共16个文件
    //个人觉得这里应该是只取最后8位，共256个文件吧？？？
    //应该是return queueName.hashCode() & 0x0f;
    int hashFile(String queueName) {
        // return queueName.hashCode() & 0xff;
        return queueName.hashCode() & 0x0f;
        //return 0;
    }

    //block的大小为1024，根据当前文件已经存在的写的位置,找到下一个比该位置大的,且是1024的倍数
    public long getLeastBlockPosition(long length) {
        if (length == 0) {
            return 0;
        }
        int initSize = 1 << 10;
        int i = 1;
        while (i * initSize <= length) {
            i++;
        }
        //定义到可用的块的第一个位置
        return i * initSize;
    }

//    public static void main(String[] args) {
//        System.out.println(getLeastBlockPosition(2048));
//    }


}