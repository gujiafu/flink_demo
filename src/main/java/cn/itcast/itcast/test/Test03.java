package cn.itcast.itcast.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Author itcast
 * Desc
 */
public class Test03 {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(0);//白球
            list.add(1);//黑球
        }
        Random random = new Random();
        while (list.size() > 1) {
            Integer i1 = list.get(random.nextInt(list.size()));
            Integer i2 = list.get(random.nextInt(list.size()));
            list.remove(i1);//移除该对象
            list.remove(i2);//移除该对象
            list.add(i1 ^ i2);
        }
        System.out.println(list);
    }
}
