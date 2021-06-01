package com.jd.janus.processor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public abstract class MessageProcessor<T,R> {
    private ThreadPoolExecutor threadPoolExecutor;

    public MessageProcessor() {
        this.threadPoolExecutor = new ThreadPoolExecutor(8,16,1000L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setPriority(5)
                        .setNameFormat("消息处理线程-%d")
                        .build(),
                         new ThreadPoolExecutor.CallerRunsPolicy()
                );
    }

    public Future<R> processMessage(T t){
        return  threadPoolExecutor.submit(new Callable<R>() {
            @Override
            public R call() throws Exception {
                try{
                    return  process(t);
                }catch (Exception e){
                    log.error("处理消息信息出错了");
                }
                return null;
            }
        });
    }

    public abstract R process(T t);

    public void close(){
        log.info("shutdown链接池信息...");
        threadPoolExecutor.shutdown();
    }
}
