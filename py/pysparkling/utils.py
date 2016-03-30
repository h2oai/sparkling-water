from h2o.frame import H2OFrame
from h2o.connection import H2OConnection

class FrameConversions:

    @staticmethod
    def _as_h2o_frame_from_RDD_String(h2oContext, rdd, framename):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDString(rdd._to_java_object_rdd(), framename)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_RDD_Bool(h2oContext, rdd, framename):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDBool(rdd._to_java_object_rdd(), framename)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_RDD_Int(h2oContext, rdd, framename):
        return FrameConversions._as_h2o_frame_from_RDD_Long(h2oContext,rdd, framename)

    @staticmethod
    def _as_h2o_frame_from_RDD_Double(h2oContext, rdd, framename):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDDouble(rdd._to_java_object_rdd(), framename)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_RDD_Float(h2oContext, rdd, framename):
        return FrameConversions._as_h2o_frame_from_RDD_Double(h2oContext,rdd, framename)

    @staticmethod
    def _as_h2o_frame_from_RDD_Long(h2oContext, rdd, framename):
        j_h2o_frame = h2oContext._jhc.asH2OFrameFromRDDLong(rdd._to_java_object_rdd(), framename)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_dataframe(h2oContext, dataframe, framename):
        if dataframe.count() == 0:
            raise ValueError('Cannot transform empty H2OFrame')
        j_h2o_frame = h2oContext._jhc.asH2OFrame(dataframe._jdf, framename)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def _as_h2o_frame_from_complex_type(h2oContext,dataframe, framename):
        # Creates a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.
        # On scala backend, to transform RDD of Product to H2OFrame, we need to know Type Tag.
        # Since there is no alternative for Product class in Python, we first transform the rdd to dataframe
        # and then transform it to H2OFrame.
        df = h2oContext._sqlContext.createDataFrame(dataframe)
        j_h2o_frame = h2oContext._jhc.asH2OFrame(df._jdf)
        j_h2o_frame_key = j_h2o_frame.key()
        return H2OFrame.from_java_h2o_frame(j_h2o_frame,j_h2o_frame_key)

    @staticmethod
    def init_scala_int_session():
        res = H2OConnection.post("scalaint")
        session_id = res.json()["session_id"]
        return session_id