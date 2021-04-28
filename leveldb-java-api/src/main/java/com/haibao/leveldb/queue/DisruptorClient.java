package com.haibao.leveldb.queue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.concurrent.ThreadFactory;

/**
 * Disruptor 操作
 *
 * @author ml.c
 * @date 2:10 PM 4/28/21
 **/
public class DisruptorClient {

    //init disruptor
    public static BaseObjectEventProducer producer = null;

    static {
        // Executor that will be used to construct new threads for consumers
        ThreadFactory guavaThreadFactory = new ThreadFactoryBuilder().setNameFormat("silk-pool-").build();
        // The factory for the event
        ObjectEventFactory factory = new ObjectEventFactory();
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;
        // Construct the Disruptor

        // Disruptor 定义了 com.lmax.disruptor.WaitStrategy 接口用于抽象 Consumer 如何等待新事件，这是策略模式的应用。Disruptor 提供了多个 WaitStrategy 的实现，每种策略都具有不同性能和优缺点，根据实际运行环境的 CPU 的硬件特点选择恰当的策略，并配合特定的 JVM 的配置参数，能够实现不同的性能提升。
        //● 其中，Disruptor默认的等待策略是BlockingWaitStrategy，这个策略的内部使用一个锁和条件变量来控制线程的执行和等待（Java基本的同步方法），BlockingWaitStrategy 是最低效的策略，但其对CPU的消耗最小并且在各种不同部署环境中能提供更加一致的性能表现；
        //● SleepingWaitStrategy 的性能表现跟 BlockingWaitStrategy 差不多，对 CPU 的消耗也类似，但其对生产者线程的影响最小，它的方式是循环等待并且在循环中间调用LockSupport.parkNanos(1)来睡眠，（在Linux系统上面睡眠时间60µs）.然而，它的优点在于生产线程只需要计数，而不执行任何指令。并且没有条件变量的消耗。但是，事件对象从生产者到消费者传递的延迟变大了。SleepingWaitStrategy最好用在不需要低延迟，而且事件发布对于生产者的影响比较小的情况下。比如异步日志功能；
        //● YieldingWaitStrategy 的性能是最好的，适合用于低延迟的系统。这种策略在减低系统延迟的同时也会增加CPU运算量。YieldingWaitStrategy策略会循环等待sequence增加到合适的值。循环中调用Thread.yield()允许其他准备好的线程执行。在要求极高性能且事件处理线数小于 CPU 逻辑核心数的场景中，推荐使用YieldingWaitStrategy策略。例如，CPU开启超线程的时候。
        //● BusySpinWaitStrategy是性能最高的等待策略，同时也是对部署环境要求最高的策略。这个性能最好用在事件处理线程比物理内核数目还要小的时候。例如：在禁用超线程技术的时候。
        Disruptor<ObjectEvent> disruptor = new Disruptor<ObjectEvent>(
                factory, bufferSize, guavaThreadFactory, ProducerType.SINGLE,
                new YieldingWaitStrategy());

        // Connect the handler 可定义多个handler，并定义执行顺序
        disruptor.handleEventsWith(new LeveldbEventHandler())
                .then(new ClearingEventHandler());

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<ObjectEvent> ringBuffer = disruptor.getRingBuffer();

        producer = new BaseObjectEventProducer(ringBuffer);
    }

}
