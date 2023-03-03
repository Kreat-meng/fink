package bean;

/**
 * @author MengX
 * @create 2023/2/27 20:29:23
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器：用于接收水位数据
 *
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    private String id;

    private Long ts;

    private Integer vc;
}
