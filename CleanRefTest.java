import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import sun.misc.Unsafe;

@SuppressWarnings("all")
public class CleanRefTest {

    private static Unsafe unsafe;
    private static ReferenceQueue<T> refQueue;

    public static void main(String[] args) {
        unsafe = getUnsafe();
        refQueue = new ReferenceQueue<>();
        System.out.println(unsafe);

        new Thread(
            () -> {
                for (; ; ) {
                    try {
                        final Reference<? extends T> remove = refQueue.remove();
                        System.out.println("ref=" + remove);
                        if (remove instanceof TRef) {
                            ((TRef)remove).clean();
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            },
            "T-Cleaner"
        ).start();
        test();
    }

    private static Unsafe getUnsafe() {
        try {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe)field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Cannot get unsafe instance.", e);
        }
    }

    private static void test() {
        for (; ; ) {
            try {
                newObj();

                //System.gc();
                Thread.sleep(10);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private static void newObj() {
        final T t = new T();
        new TRef(t, refQueue);
    }

    private static class T {
        private long addr;

        T() {
            addr = unsafe.allocateMemory(100);
            System.out.println("allocate mem");
        }

        public long getAddr() {
            return addr;
        }
    }

    private static class TRef extends PhantomReference<T> {
        private static final Set<TRef> SET = Collections.newSetFromMap(new ConcurrentHashMap<>());

        private long addr;

        public TRef(T referent, ReferenceQueue<? super T> q) {
            super(referent, q);
            addr = referent.getAddr();
            SET.add(this);
        }

        public void clean() {
            SET.remove(this);
            unsafe.freeMemory(addr);
            System.out.println("free mem");
        }
    }
}
