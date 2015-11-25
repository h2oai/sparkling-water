import urllib
from h2o import h2o
from h2o.frame import H2OFrame
from h2o.connection import H2OConnection
import pysparkling.dataframe


class SparklingWaterConnection:
    def __init__(self, ip, port, initialize = True):
        self.ip = ip
        self.port = port
        # initialize the http connection, needed for the communication via REST endpoints
        if initialize:
            h2o.init(ip=ip, port=port)

    # Transform DataFrame to H2OFrame
    def as_h2o_frame(self, dataframe):
        if not isinstance(dataframe, dataframe.DataFrame):
            raise TypeError
        h2oframe = self.as_h2o_frame_by_id(dataframe._id)
        return h2oframe

    # Transform DataFrame to H2OFrame where DataFrame is specified by its id
    def as_h2o_frame_by_id(self, dataframe_id):
        res = h2o.H2OConnection.post("dataframes/" + urllib.quote(dataframe_id) + "/h2oframe").json()
        h2oframe = H2OFrame.get_frame(res["h2oframe_id"])
        return h2oframe

    # Transform H2OFrame to DataFrame
    def as_data_frame(self, h2o_frame):
        if not isinstance(h2o_frame, H2OFrame):
            raise TypeError
        dataframe = self.as_dataframe_by_id(h2o_frame._id)
        return dataframe

    # Transform H2OFrame to DataFrame where H2OFrame is specified by its id
    def as_dataframe_by_id(h2o_frame_id):
        res = h2o.H2OConnection.post("h2oframes/" + urllib.quote(h2o_frame_id) + "/dataframe")
        data_frame_id = res.json()["dataframe_id"]
        data_frame = pysparkling.dataframe.DataFrame.get_data_frame(data_frame_id)
        return data_frame

    def init_scala_int_session(self):
        res = h2o.H2OConnection.post("scalaint")
        session_id = res.json()["session_id"]
        return session_id
