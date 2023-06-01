import subprocess
from pysparkling.conf import H2OConf


def createH2OConf():
    conf = H2OConf()
    conf.setClusterSize(1)
    conf.useAutoClusterStart()
    conf.setExternalClusterMode()
    conf.setLogLevel("INFO")
    return conf


def killAllYarnApps():
    command = "yarn application --list | awk '{print $1}' | grep application | xargs yarn application -kill"
    return str(subprocess.check_output(command, shell=True))


def listYarnApps():
    return str(subprocess.check_output("yarn application -list", shell=True))


def noYarnApps():
    return specificNumberOfYarnApps(0)


def specificNumberOfYarnApps(num):
    exp = "Total number of applications (application-types: [], states: [SUBMITTED, ACCEPTED, RUNNING] and tags: []):"
    exp += str(num)
    return exp in listYarnApps()


def yarnLogs(appId):
    return str(subprocess.check_output("yarn logs -applicationId " + appId, shell=True))


def getYarnAppIdFromNotifyFile(path):
    with open(path, 'r') as f:
        return f.readlines()[1].replace("job", "application").strip()


def getIpPortFromNotifyFile(path):
    with open(path, 'r') as f:
        return f.readlines()[0].strip()
