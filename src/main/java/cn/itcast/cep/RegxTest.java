package cn.itcast.cep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Date 2021
 */
public class RegxTest {

    public static void main(String[] args) {

        //判断字符串是否包含数字
        String str = "wew1111rwere";

        //定义正则表达式规则
        Pattern pattern = Pattern.compile("\\d+");
        //匹配规则
        Matcher matcher = pattern.matcher(str);

        //数据判断
        if(matcher.find()){
            System.out.println("有数字");
        }else{
            System.out.println("无数字");
        }
    }
}
