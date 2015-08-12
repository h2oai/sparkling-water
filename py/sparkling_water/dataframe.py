import h2o
import urllib, json
from pyspark.sql.types import *
from h2o import *
import utils

class DataFrame:
    def __init__(self):
        self._id = ""

    @staticmethod
    def get_data_frame(data_frame_id):
        res = h2o.H2OConnection.post("dataframes/" + urllib.quote(data_frame_id)).json()
        dataframe = DataFrame()
        dataframe._id = res["dataframe_id"]
        dataframe._schema = StructType.fromJson(json.loads(res["schema"]))
        dataframe._fields = [field.name for field in dataframe._schema.fields]
        dataframe._ncols = len(dataframe._schema.fields)
        return dataframe

    def get_fields(_self):
        return _self._fields

    def get_id(_self):
        return _self._id

    def get_schema(_self):
        return _self._schema

    def get_num_fields(_self):
        return _self._ncols

    # Converts the DataFrame to H2OFrame and then downloads the dataset as CSV file
    def download(_self, filename):
        h2o_frame = utils.Utils.dataframe_2_h2oframe(_self)
        h2o.download_csv(h2o_frame, filename)
