import urllib, urllib2, csv, os, tempfile, subprocess, sys, site, time
from h2o import h2o
from h2o.frame import H2OFrame
from h2o.connection import H2OConnection
from pyspark import SparkConf, SparkContext
import dataframe

class H2OContext:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        # initialize the http connection, needed for the communication via REST endpoints
        h2o.init(ip=ip,port=port)
        self.scala_session_id = H2OContext.init_scala_int_session()

    def as_h2o_frame(self, data_frame):
        return Utils.dataframe_2_h2oframe(data_frame)

    def as_data_frame(self, h2o_frame):
        return Utils.h2oframe_2_dataframe(h2o_frame)

    def stop(self):
        try:
            res = h2o.H2OConnection.post("scalaint/"+str(self.scala_session_id), code='h2oContext.stop(true)')
        except Exception:
            sys.exc_clear()

    @staticmethod
    def init_scala_int_session():
        res = h2o.H2OConnection.post("scalaint")
        session_id = res.json()["session_id"]
        return session_id

    @staticmethod
    def __get_local_ip_port(process, file_closed):
        last_pos = 0
        while process.poll() is None:
            # p.poll() returns None while the program is still running
            # sleep for 1 second
            time.sleep(1)
            if Utils.is_empty_file(file_closed):
                continue
            import mmap
            file_opened = open(file_closed)
            f = mmap.mmap(file_opened.fileno(), 0, access=mmap.ACCESS_READ)

            if f.find('Open H2O Flow in browser') != -1:
                content = file_opened.read()
                print content
                ip_port = content.split("Open H2O Flow in browser: http://")[1].split(" (CMD + click in Mac OSX)")[0].split(":")
                return ip_port
                break

    @staticmethod
    def init_h2o_context_locally():
        jar_path = None
        jarpaths = [os.path.join(sys.prefix, "sparkling_water_jar", "sparkling-water-all.jar"),
                    os.path.join(os.path.sep,"usr","local","sparkling_water_jar","sparkling-water-all.jar"),
                    os.path.join(sys.prefix, "local", "sparkling_water_jar", "sparkling-water-all.jar"),
                    os.path.join(site.USER_BASE, "sparkling_water_jar", "sparkling-water-all.jar")
                    ]
        if os.path.exists(jarpaths[0]):   jar_path = jarpaths[0]
        elif os.path.exists(jarpaths[1]): jar_path = jarpaths[1]
        elif os.path.exists(jarpaths[2]): jar_path = jarpaths[2]
        else:                             jar_path = jarpaths[3]

        cmd = [os.environ['SPARK_HOME']+"/bin/spark-submit",
        "--class", "water.SparklingWaterDriver",
        "--master", "local-cluster[1,1,512]",
        "--driver-class-path", jar_path,
        "--conf", "spark.driver.extraJavaOptions=\"-XX:MaxPermSize=384m\"",
        jar_path]

        cwd = os.path.abspath(os.getcwd())

        stdout_name = H2OConnection._tmp_file("stdout")
        stdout_file = open(stdout_name, "w+b")
        stderr_name = H2OConnection._tmp_file("stderr")
        stderr_file = open(stderr_name, "w+b")

        ip_port = []
        if sys.platform == "win32":
            p = subprocess.Popen(args=cmd, stdout=stdout_file, stderr=stderr_file, cwd=cwd, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            ip_port = H2OContext.__get_local_ip_port(p, stderr_name)
        else:
            p = subprocess.Popen(args=cmd, stdout=stdout_file, stderr=stderr_file, cwd=cwd, preexec_fn=os.setsid)
            ip_port = H2OContext.__get_local_ip_port(p, stderr_name)
        return H2OContext(ip_port[0], ip_port[1])



class Utils:

    @staticmethod
    def is_empty_file(file_closed):
        return os.stat(file_closed).st_size == 0

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
