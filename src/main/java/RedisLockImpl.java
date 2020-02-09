import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RedisLockImpl implements Lock {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 你要的锁的资源
     */
    private String resourceName;

    int timeout;

    public RedisLockImpl(StringRedisTemplate stringRedisTemplate, String resourceName, int timeout) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.resourceName = "lock_" + resourceName;
        this.timeout = timeout;
    }

    private Lock lock = new ReentrantLock();

    /**
     * 一直要等到抢到锁为止
     */
    @Override
    public void lock() {
        // 限制同一个JVM进程内的资源竞争,分布式锁（JVM之间的竞争） + JVM锁
        lock.lock();
        try {
            while (!tryLock()) {
                // 订阅指定的Redis主题，接收释放锁的信号
                stringRedisTemplate.execute(new RedisCallback<Long>() {
                    @Override
                    public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                        try {
                            CountDownLatch countDownLatch = new CountDownLatch(1);
                            // subscribe立马返回，是否订阅完毕，异步触发
                            redisConnection.subscribe((message, pattern) -> {
                                // 收到消息，不管结果，立刻再次抢锁
                                countDownLatch.countDown();
                            }, ("release_lock_" + resourceName).getBytes());
                            //等待有通知，才继续循环
                            countDownLatch.await(timeout, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return 0L;
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        // set命令，往redis存放锁的标记
        Boolean lockResult = stringRedisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                String value = "";
                Boolean result = redisConnection.set(resourceName.getBytes(), value.getBytes(), Expiration.seconds(timeout), RedisStringCommands.SetOption.SET_IF_ABSENT);
                return result;
            }
        });
        return lockResult;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        stringRedisTemplate.delete(resourceName);
        //通过Redis发布订阅机制，发送一个通知给其他等待的请求
        stringRedisTemplate.execute(new RedisCallback<Long>() {
            @Override
            public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.publish(("release_lock_" + resourceName).getBytes(), resourceName.getBytes());
            }
        });
    }

    @Override
    public Condition newCondition() {
        return null;
    }
}
