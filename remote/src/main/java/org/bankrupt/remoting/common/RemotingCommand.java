package org.bankrupt.remoting.common;

import com.alibaba.fastjson.JSON;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义协议类型
 */
public class RemotingCommand {
    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    //获得唯一的id
    private static AtomicInteger requestId = new AtomicInteger(0);
    /**
     * 自定义协议 没有header 和 body
     * 4: 总长度 14 + header + body
     * 4: requestId
     * 4: code
     * 1: body的序列化协议
     * 1: direction
     */
    public static final int FIX_COMMON_LENGTH = 4 + 4 + 4 + 1 + 1;
    /**
     * 信息id
     */
    private int id;
    /**
     * 信息类型
     */
    private int code;

    /**
     * 协议类型(body的序列化协议)
     */
    private byte protocol;

    /**
     * 传输方向  0：请求  1：返回
     */
    private byte direction;
    /**
     * 自定义header
     */
    private Map<String, String> headers;

    /**
     * 内容
     */
    private transient byte[] body;

    public static Map<String, String> decodeHeaders(byte[] headerByteArray) {
        return JSON.parseObject(headerByteArray, Map.class);
    }

    public static RemotingCommand decode(ByteBuffer buf) {
        int count = RemotingCommand.FIX_COMMON_LENGTH;
        int len = buf.getInt();//消息的自增id 4
        int id = buf.getInt();//消息的自增id 4
        int code = buf.getInt();//消息的code 4
        byte protocol = buf.get();//protocol 协议类型 1
        byte direction = buf.get();//入的 1
        int headerLength = buf.getInt();//header的长度 1
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setCode(code);
        remotingCommand.setId(id);
        remotingCommand.setProtocol(protocol);
        remotingCommand.setDirection(direction);
        if (headerLength != 0) {
            count += headerLength;
            byte[] headerByteArray = new byte[headerLength];
            buf.get(headerByteArray);
            Map<String, String> headers = RemotingCommand.decodeHeaders(headerByteArray);
            remotingCommand.setHeaders(headers);
        }
        int bodyLength = len - count;
        byte[] body = new byte[bodyLength];
        buf.get(body);
        remotingCommand.setBody(body);
        return remotingCommand;
    }

    public static byte[] encode(Object msg) {
        return JSON.toJSONBytes(msg);
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public byte getProtocol() {
        return protocol;
    }

    public void setProtocol(byte protocol) {
        this.protocol = protocol;
    }

    public byte getDirection() {
        return direction;
    }

    public void setDirection(byte direction) {
        this.direction = direction;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] encodeHeaders() {
        return encodeHeader();
    }
//
//    public ByteBuffer encodeHeaders() {
//        return encodeHeader();
//    }


    private byte[] encodeHeader() {
        return headerEncode(this.headers);
    }

//    private ByteBuffer encodeHeader() {
//        byte[] header = headerEncode(this.headers);
//        int totalLength = RemotingCommand.FIX_COMMON_LENGTH + header.length + this.getBody().length;
//        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
//        buffer.putInt(totalLength);//4
//        buffer.putInt(this.getId());//4
//        buffer.putInt(this.getCode());//4
//        buffer.put(this.getProtocol());//1
//        buffer.put(this.getDirection());//1
//        buffer.putInt(header.length);//header的长度//4
//        buffer.put(header);//header
//        buffer.flip();
//        return buffer;
//    }

    private byte[] headerEncode(Map<String, String> headers) {
        return JSON.toJSONBytes(headers);
    }



    public static int createRequestId() {
        return requestId.incrementAndGet();
    }

    public static <T> T doDecode(byte[] bytes, Class<T> clazz) {
        return JSON.parseObject(bytes, clazz);
    }


    @Override
    public String toString() {
        return "RemotingCommand{" +
                "id=" + id +
                ", code=" + code +
                ", protocol=" + protocol +
                ", direction=" + direction +
                ", headers=" + headers +
                '}';
    }
}
