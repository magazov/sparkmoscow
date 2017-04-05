package taxi;

/**
 * Created by Evegeny on 05/04/2017.
 */
public interface FunctionUtils {
    static Trip conertLineToTrip(String line) {
        String[] strings = line.split(" ");
        return new Trip(strings[0], strings[1], Integer.valueOf(strings[2]));

    }
}
