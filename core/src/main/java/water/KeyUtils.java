package water;

public class KeyUtils {
  public static <X extends Keyed> Key<X> make(String s) {
    Key k = water.Key.make(s);
    return (Key<X>) k;
  }
}
