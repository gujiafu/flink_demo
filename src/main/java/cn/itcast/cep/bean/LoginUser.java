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
public class LoginUser {
    //    new LoginUser (1, "192.168.0.1", "fail", 1558430842000L),
    private Integer id;
    private String ip;
    private String status;
    private Long eventTime;
}
