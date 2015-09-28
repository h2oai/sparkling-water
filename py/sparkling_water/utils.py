import urllib, urllib2, csv, os, tempfile
from h2o import h2o
from h2o.frame import H2OFrame
from pyspark import SparkConf, SparkContext
import dataframe
class Utils:
    # Transform H2OFrame to DataFrame
    @staticmethod
    def h2oframe_2_dataframe(h2o_frame):
        if not isinstance(h2o_frame, H2OFrame):
            raise TypeError
        dataframe = Utils.h2oframe_2_dataframe_by_id(h2o_frame._id)
        return dataframe

    # Transform H2OFrame to DataFrame. H2OFrame is identified by its id.
    @staticmethod
    def h2oframe_2_dataframe_by_id(h2o_frame_id):
        res = h2o.H2OConnection.post("h2oframes/" + urllib.quote(h2o_frame_id) + "/dataframe")
        data_frame_id = res.json()["dataframe_id"]
        data_frame = dataframe.DataFrame.get_data_frame(data_frame_id)
        return data_frame

    @staticmethod
    def dataframe_2_h2oframe(dataframe):
        if not isinstance(dataframe, dataframe.DataFrame):
            raise TypeError
        h2oframe = Utils.dataframe_2_h2oframe_by_id(dataframe._id)
        return h2oframe

    @staticmethod
    def dataframe_2_h2oframe_by_id(dataframe_id):
        res = h2o.H2OConnection.post("dataframes/" + urllib.quote(dataframe_id) + "/h2oframe").json()
        h2oframe = H2OFrame.get_frame(res["h2oframe_id"])
        return h2oframe

