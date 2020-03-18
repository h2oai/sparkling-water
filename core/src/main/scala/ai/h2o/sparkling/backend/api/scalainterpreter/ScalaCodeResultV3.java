package ai.h2o.sparkling.backend.api.scalainterpreter;

import water.Iced;
import water.Key;
import water.api.schemas3.KeyV3;

public class ScalaCodeResultV3 extends KeyV3<Iced, ScalaCodeResultV3, ScalaCodeResult> {
    public ScalaCodeResultV3() {
    }

    public ScalaCodeResultV3(Key<ScalaCodeResult> key) {
        super(key);
    }
}