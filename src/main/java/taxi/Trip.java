package taxi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Evegeny on 05/04/2017.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Trip {

    private String driverId;
    private String city;
    private int distance;
}
