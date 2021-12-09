import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class WorkerDemo {
    public static void main(String[] args) {
        new CountDownLatchWorkDemo().mainTask();
        new CallbackWorkDemo().mainTask();
    }
}

class CountDownLatchWorkDemo {
    public void mainTask() {

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        // 注意，这里就会造成伪共享（false sharing）
        final Integer[] results = new Integer[2];

        // 第一个任务
        new Thread(() -> {
            try {
                results[0] = task1();
            } finally {
                countDownLatch.countDown();
            }
        }).start();

        // 第二个任务
        new Thread(() -> {
            try {
                results[1] = task2();
            } finally {
                countDownLatch.countDown();
            }
        }).start();

        try {
            // 等待
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new Error("WTF", e);
        }

        final int sum = Stream.of(results).mapToInt(t -> t).sum();
        System.out.println(sum);
    }

    private Integer task1() {
        // 模拟任务耗时
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        return 1;
    }

    private Integer task2() {
        // 模拟任务耗时
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }

        return 2;
    }
}

class CallbackWorkDemo {
    public void mainTask() {

        // 注意，这里就会造成伪共享（false sharing）
        final Integer[] results = new Integer[2];

        // 任务执行完成后的回调函数
        final Runnable callback = () -> {
            final int sum = Stream.of(results).mapToInt(t -> t).sum();
            System.out.println(sum);
        };

        final AtomicInteger counter = new AtomicInteger(0);
        final Runnable finishWork = () -> {
            if (Objects.equals(2, counter.incrementAndGet())) {
                callback.run();
            }
        };

        // 第一个任务
        new Thread(() -> {
            try {
                results[0] = task1();
            } finally {
                finishWork.run();
            }
        }).start();

        // 第二个任务
        new Thread(() -> {
            try {
                results[1] = task2();
            } finally {
                finishWork.run();
            }
        }).start();
    }

    private Integer task1() {
        // 模拟任务耗时
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        return 1;
    }

    private Integer task2() {
        // 模拟任务耗时
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }

        return 2;
    }
}
