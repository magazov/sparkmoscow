package sparkWithSpring.words;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Evegeny on 05/04/2017.
 */
@Component
public class GarbageBag implements Serializable {
    public List<String> garbage;

    @Value("${garbage}")
    private void setGarbage(String[] garbage) {
        this.garbage = Arrays.asList(garbage);
    }

}








