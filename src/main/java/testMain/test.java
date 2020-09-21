package testMain;

import com.google.inject.internal.cglib.core.$Local;

public class test {
    public long count = 0;
    public long add10K() {
        int idx = 0;
        while (idx++ < 10000) {
            count += 1;
        }
        System.out.println(count);
        return count;
    }
}