package cn.itcast.cep.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Date 2020/11/2
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message {
    //new Message("1", "TMD", 1558430842000L),//2019-05-21 17:27:22
    private String id;
    private String msg;
    private Long eventTime;

}
