package ai.h2o.sparkling.backend.api.scalainterpreter;

import water.Key;
import water.Lockable;

/**
 * This object is returned by jobs executing the Scala code
 */
public class ScalaCodeResult extends Lockable<ScalaCodeResult> {
    public String code;
    public String scalaStatus;
    public String scalaResponse;
    public String scalaOutput;

    /**
     * Create a Lockable object, if it has a {@link Key}.
     *
     * @param key key
     */
    public ScalaCodeResult(Key<ScalaCodeResult> key) {
        super(key);
    }

    @Override
    public Class<ScalaCodeResultV3> makeSchema() {
        return ScalaCodeResultV3.class;
    }
}
