package cn.itcast.test;

/**
 * Author itcast
 * Desc
 */
public class Test02 {
    public static void main(String[] args) {
        int result = 0;
        int[] arr1 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};//完整的
        int[] arr2 = new int[]{1, 2, 3, 4, 6, 7, 8, 9, 10, 0 };//丢失了一个数的

        for (int i = 0; i < arr1.length; i++) {
            if (i == 0) {
                result = arr1[i] ^ arr2[i];
            } else {
                //result = result ^ (arr1[i] ^ arr2[i]);
                result ^= (arr1[i] ^ arr2[i]);
            }
        }

        System.out.println("丢失的数为:" + result);
    }
}
