package com.airlook.data.archive;

import com.alibaba.fastjson.JSON;
import com.amazonaws.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * description: BinlogTest <br>
 * date: 2020/3/25 10:04 <br>
 * author: Lsq <br>
 * version: 1.0 <br>
 */
public class BinlogReader {

    // private static Logger log = LoggerFactory.getLogger(BinlogReader.class);

    public static final byte[] MAGIC_HEADER = new byte[]{(byte) 0xfe, (byte) 0x62, (byte) 0x69, (byte) 0x6e};

    public static void main(String[] args){

        // 读 binlog
        String filePath = "C:\\Users\\EDZ\\Desktop\\mysql_bin.000001";
        File binlogFile = new File(filePath);
        FileInputStream fileInputStream = null;

        try {
            fileInputStream = new FileInputStream(binlogFile);

            // 将流全部拉取到内存中
            byte[] bytes = IOUtils.toByteArray(fileInputStream);

            // 包装一层
            MyByteArrayInputStream dataInputStream = new MyByteArrayInputStream(bytes);

            // 检查文件是否是 binlog
            checkFileIsBinlog(dataInputStream);

            // 打印第一条可用的事件信息
            EventHeader eventHeader = printCommonEventInfo(dataInputStream);

            // 清空流中的下标
            // dataInputStream.reset();

            // 跳过魔数和通用事件头
            // dataInputStream.mark((int)eventHeader.getEventLength() - 19 - 4);

            printEventInfoByType(dataInputStream, eventHeader, (int)eventHeader.getEventLength() - 19 - 4);

        } catch (FileNotFoundException e) {
            // log.warn("路径[{}]下未找到文件, 具体原因[{}]", filePath, e);
            System.out.println("路径[" + filePath + "]下未找到文件。");
        } catch (IOException e) {
            // log.warn("路径[{}]下读取文件流异常，具体原因[{}]", filePath, e);
            System.out.println("路径[" + filePath + "]下读取文件流失败。");
        } finally {
            if (null != fileInputStream){
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    // log.warn("Emmm，关闭文件流失败，文件路径[{}]。",filePath);
                    System.out.println("Emmm，关闭文件流失败，文件路径[" + filePath + "]。");
                }
            }
        }
    }

    // 属性	        字节数	    含义
    // timestamp	    4	    包含了该事件的开始执行时间
    // eventType	    1	    事件类型
    // serverId	        4	    标识产生该事件的MySQL服务器的server-id
    // eventLength	    4	    该事件的长度(Header+Data+CheckSum)
    // nextPosition	    4	    下一个事件在binlog文件中的位置
    // flags	        2	    标识产生该事件的MySQL服务器的server-id。
    // EventHeader{timestamp=1585042041000, eventType=15, serverId=1, eventLength=119, nextPosition=123, flags=0}
    private static EventHeader printCommonEventInfo(MyByteArrayInputStream dataInputStream) {
        // 魔数占位符 4 位，当前事件 119 字节。下个事件 123 没毛病
        EventHeader eventHeader = new EventHeader();
        eventHeader.setTimestamp(dataInputStream.readLong(4) * 1000L);
        eventHeader.setEventType(dataInputStream.readInt(1));
        eventHeader.setServerId(dataInputStream.readLong(4));
        eventHeader.setEventLength(dataInputStream.readLong(4));
        eventHeader.setNextPosition(dataInputStream.readLong(4));
        eventHeader.setFlags(dataInputStream.readInt(2));
        // log.info("此次获得的事件信息是：[{}]", JSON.toJSONString(eventHeader));
        System.out.println("此次获得的事件信息是：[" + JSON.toJSONString(eventHeader) + "]");
        return eventHeader;
    }

    // https://dev.mysql.com/doc/internals/en/binlog-event-type.html
    // 0x0f(15)  FORMAT_DESCRIPTION_EVENT

    // https://dev.mysql.com/doc/internals/en/format-description-event.html
    // 2                binlog-version              binlog版本
    // string[50]       mysql-server version        服务器版本
    // 4                create timestamp            binlog文件的创建时间
    // 1                event header length         时间投长度，为 19 上面解析过
    // string[p]        event type header length    一个数组，标识所有事件的私有事件头的长度
    // 119 - 19(通用头) - 4(校验位) - 2 - 50 - 4 - 1 = 39
    private static void printEventInfoByType(MyByteArrayInputStream dataInputStream, EventHeader eventHeader, int lengh) {
        FormatDescriptionEventData descriptionEventData = new FormatDescriptionEventData();
        descriptionEventData.setBinlogVersion(dataInputStream.readInt(2));
        descriptionEventData.setServerVersion(dataInputStream.readString(50).trim());
        descriptionEventData.setTimestamp(dataInputStream.readLong(4) * 1000L);
        descriptionEventData.setHeaderLength(dataInputStream.readInt(1));
        int sums = dataInputStream.available();
        for (int i = 0; i < sums; i++) {
            descriptionEventData.getHeaderArrays().add(dataInputStream.readInt(1));
        }
        System.out.println("此次获得的事件信息是：[" + JSON.toJSONString(descriptionEventData) + "]");
    }

    private static void checkFileIsBinlog(MyByteArrayInputStream dataInputStream) {
        byte[] magicHeader = dataInputStream.read(4);
        if(!Arrays.equals(MAGIC_HEADER, magicHeader)){
            throw new RuntimeException("binlog文件格式不对");
        }else {
            // log.info("魔数\\xfe\\x62\\x69\\x6e是否正确:" + Arrays.equals(MAGIC_HEADER, magicHeader));
            System.out.println("魔数\\xfe\\x62\\x69\\x6e是否正确:" + Arrays.equals(MAGIC_HEADER, magicHeader));
        }
    }

    public static class MyByteArrayInputStream extends ByteArrayInputStream {

        public MyByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public byte[] read(int size){
            byte[] b = new byte[size];
            read(b, 0, b.length);
            return b;
        }

        public long readLong(int size){
            byte[] b = new byte[8];
            read(b, 0, size);
            return byteToLong(b);
        }

        public int readInt(int size){
            byte[] b = new byte[4];
            read(b, 0, size);
            return bytesToInt(b);
        }

        public String readString(int size){
            byte[] b = new byte[size];
            read(b, 0, size);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < b.length; i++) {
                byte b1 = b[i];
                sb.append(b1);
            }
            return sb.toString();
        }

        /**
         * @方法功能 字节数组和整型的转换
         * @return 整型
         */
        public static int bytesToInt(byte[] bytes) {
            int num = bytes[0] & 0xFF;
            num |= ((bytes[1] << 8) & 0xFF00);
            num |= ((bytes[2] << 16) & 0xFF0000);
            num |= ((bytes[3] << 24) & 0xFF000000);
            return num;
        }

        /**
         * @方法功能 字节数组和长整型的转换
         * @return 长整型
         */
        public static long byteToLong(byte[] b) {
            long s = 0;
            long s0 = b[0] & 0xff;// 最低位
            long s1 = b[1] & 0xff;
            long s2 = b[2] & 0xff;
            long s3 = b[3] & 0xff;
            long s4 = b[4] & 0xff;// 最低位
            long s5 = b[5] & 0xff;
            long s6 = b[6] & 0xff;
            long s7 = b[7] & 0xff; // s0不变
            s1 <<= 8;
            s2 <<= 16;
            s3 <<= 24;
            s4 <<= 8 * 4;
            s5 <<= 8 * 5;
            s6 <<= 8 * 6;
            s7 <<= 8 * 7;
            s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
            return s;
        }

    }

    public static class FormatDescriptionEventData {

        private int binlogVersion;
        private String serverVersion;
        private long timestamp;
        private int headerLength;
        private List headerArrays = new ArrayList<Integer>();

        public int getBinlogVersion() {
            return binlogVersion;
        }

        public void setBinlogVersion(int binlogVersion) {
            this.binlogVersion = binlogVersion;
        }

        public String getServerVersion() {
            return serverVersion;
        }

        public void setServerVersion(String serverVersion) {
            this.serverVersion = serverVersion;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public int getHeaderLength() {
            return headerLength;
        }

        public void setHeaderLength(int headerLength) {
            this.headerLength = headerLength;
        }

        public List getHeaderArrays() {
            return headerArrays;
        }

        public void setHeaderArrays(List headerArrays) {
            this.headerArrays = headerArrays;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("FormatDescriptionEventData");
            sb.append("{binlogVersion=").append(binlogVersion);
            sb.append(", serverVersion=").append(serverVersion);
            sb.append(", timestamp=").append(timestamp);
            sb.append(", headerLength=").append(headerLength);
            sb.append(", headerArrays=").append(headerArrays);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class EventHeader {

        private long timestamp;
        private int eventType;
        private long serverId;
        private long eventLength;
        private long nextPosition;
        private int flags;

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public int getEventType() {
            return eventType;
        }

        public void setEventType(int eventType) {
            this.eventType = eventType;
        }

        public long getServerId() {
            return serverId;
        }

        public void setServerId(long serverId) {
            this.serverId = serverId;
        }

        public long getEventLength() {
            return eventLength;
        }

        public void setEventLength(long eventLength) {
            this.eventLength = eventLength;
        }

        public long getNextPosition() {
            return nextPosition;
        }

        public void setNextPosition(long nextPosition) {
            this.nextPosition = nextPosition;
        }

        public int getFlags() {
            return flags;
        }

        public void setFlags(int flags) {
            this.flags = flags;
        }

        //省略setter和getter方法
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("EventHeader");
            sb.append("{timestamp=").append(timestamp);
            sb.append(", eventType=").append(eventType);
            sb.append(", serverId=").append(serverId);
            sb.append(", eventLength=").append(eventLength);
            sb.append(", nextPosition=").append(nextPosition);
            sb.append(", flags=").append(flags);
            sb.append('}');
            return sb.toString();
        }
    }

}
