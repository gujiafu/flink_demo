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
public class Product {

    private Long goodsId;
    private Double goodsPrice;
    private String goodsName;
    private String alias;
    private Long orderTime;
    private Boolean status;
}
