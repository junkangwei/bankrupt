package org.bankrupt.broker.task;

import message.Message;
import org.apache.log4j.Logger;
import org.bankrupt.broker.BrokerController;
import org.bankrupt.broker.commitLog.DefaultMessageStore;
import org.bankrupt.broker.commitLog.MessageStore;
import org.bankrupt.broker.delay.DelayMessageService;
import org.bankrupt.broker.store.SelectMappedBufferResult;
import org.bankrupt.broker.topic.ConsumeQueue;
import org.bankrupt.common.constant.HeaderConstant;
import org.bankrupt.common.request.MessageAddRequest;
import org.bankrupt.common.util.MapUtil;
import org.bankrupt.remoting.common.RemotingCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 创建时间： 2022/7/22 10:44
 *
 * @author 魏俊康
 * 描述： TODO
 */
public class DelayedMessageTask implements Runnable{

    public static Logger log = Logger.getLogger(DelayedMessageTask.class);

    private int delayLevel;
    private long offset;
    private DelayMessageService delayMessageService;
    private DefaultMessageStore defaultMessageStore;

    public DelayedMessageTask(int delayLevel, long offset, DelayMessageService delayMessageService,DefaultMessageStore defaultMessageStore) {
        this.delayLevel = delayLevel;
        this.offset = offset;
        this.delayMessageService = delayMessageService;
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public void run() {
        //开始执行业务
        try {
            runTask();
        }catch (Exception e){
            //如果发生了异常重新投递
            delayMessageService.getScheduledExecutorService()
                    .schedule(new DelayedMessageTask(this.delayLevel,this.offset,delayMessageService,defaultMessageStore),50000L, TimeUnit.MILLISECONDS);
        }
    }

    private void runTask() {
        ConsumeQueue cq =
                defaultMessageStore.findConsumeQueue(HeaderConstant.SCHEDULE_TOPIC,
                        delayLevel - 1);

        if(cq != null){
            //说明有消息
            SelectMappedBufferResult bufferConsumeQueue = cq.getMoreMessage(this.offset);
            if(bufferConsumeQueue != null){
                int i = 0;
                long nextOffset = offset;
                for(;i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE){
                    long cmOffset = bufferConsumeQueue.getByteBuffer().getLong();
                    int size = bufferConsumeQueue.getByteBuffer().getInt();
                    long storeTime = bufferConsumeQueue.getByteBuffer().getLong();
                    //开始判断时间
                    long now = System.currentTimeMillis();
                    long countdown = storeTime - now;

                    nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                    if(countdown <= 0){
                        Message message = this.defaultMessageStore.getCommitLog().getMessage(cmOffset, size);
                        if(message != null){
                            //重新生成真实的消息
                            Message realMessage = this.newMessage(message);
                            try {
                                defaultMessageStore.getCommitLog().putMessage(realMessage);
                            } catch (Exception e) {
                                //异常不投递
                                log.error("重新投递消息失败");
//                                delayMessageService.getScheduledExecutorService()
//                                        .schedule(new DelayedMessageTask(this.delayLevel,nextOffset,delayMessageService,defaultMessageStore)
//                                                ,2000L, TimeUnit.MILLISECONDS);
//                                delayMessageService.updateOffset(this.delayLevel,
//                                        nextOffset);
                                return;
                            }
                        }
                    }else{
                        //说明消息还没到时间
                        delayMessageService.getScheduledExecutorService()
                                .schedule(new DelayedMessageTask(this.delayLevel,nextOffset,delayMessageService,defaultMessageStore)
                                        ,countdown, TimeUnit.MILLISECONDS);
                        delayMessageService.updateOffset(this.delayLevel, nextOffset);
                        return;
                    }
                }
                nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                delayMessageService.getScheduledExecutorService()
                        .schedule(new DelayedMessageTask(this.delayLevel,nextOffset,delayMessageService,defaultMessageStore)
                                ,2000, TimeUnit.MILLISECONDS);
                delayMessageService.updateOffset(this.delayLevel, nextOffset);
                return;
            }else{
                log.error("当前没有新的消息");
            }
        }
        log.error("当前没有队列可以使用");
        //如果没有当前的queue
        delayMessageService.getScheduledExecutorService().schedule(new DelayedMessageTask(this.delayLevel,
                this.offset,delayMessageService,defaultMessageStore), 2000L,TimeUnit.MILLISECONDS);
    }

    private Message newMessage(Message oldMessage) {
        Message message = new Message();
        Map<String,String> oldMap =(Map<String,String>) MapUtil.getValue(oldMessage.getHeader());
        message.setHeader(oldMessage.getHeader());
        Map<String, String> header = new HashMap<>();
        String topic = oldMap.get(HeaderConstant.REAL_TOPIC);
        header.put(HeaderConstant.TOPIC,topic);
        Integer queueId = Integer.valueOf(oldMap.get(HeaderConstant.REAL_QUEUE_ID));
        header.put(HeaderConstant.QUEUE_ID,queueId.toString());
        message.setTopic(topic);
        message.setBody(oldMessage.getBody());
        message.setBornTime(System.currentTimeMillis());
        message.setQueueId(queueId);
        String key = topic + "-" + queueId;
        Long queueOffset = defaultMessageStore.getCommitLog().getTopicQueueTable().get(key);
        if (null == queueOffset) {
            queueOffset = 0L;
            defaultMessageStore.getCommitLog().getTopicQueueTable().put(key, queueOffset);
        }
        message.setHeader(header.toString());
        message.setQueueOffset(queueOffset);
        return message;
    }


    public int getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(int delayLevel) {
        this.delayLevel = delayLevel;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public DelayMessageService getDelayMessageService() {
        return delayMessageService;
    }

    public void setDelayMessageService(DelayMessageService delayMessageService) {
        this.delayMessageService = delayMessageService;
    }
}
