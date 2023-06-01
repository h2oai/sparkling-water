import re


class VersionComponents(object):

    @staticmethod
    def parseFromSparklingWaterVersion(version):
        match = re.search(r"^((\d+\.\d+\.\d+)\.(\d+)-(\d+))(\.(\d+))?-(\d+\.\d+)$", version)
        result = VersionComponents()
        result.fullVersion = match.group(0)
        result.sparklingVersion = match.group(1)
        result.sparklingMajorVersion = match.group(2)
        result.sparklingMinorVersion = match.group(3)
        result.sparklingPatchVersion = match.group(4)
        result.nightlyVersion = match.group(6)
        result.sparkMajorMinorVersion = match.group(7)
        return result

    @staticmethod
    def parseFromPySparkVersion(version):
        match = re.search(r"^((\d+)\.(\d+))\.(\d+)(.+)?$", version)
        result = VersionComponents()
        result.fullVersion = match.group(0)
        result.sparkMajorMinorVersion = match.group(1)
        result.sparkMajorVersion = match.group(2)
        result.sparkMinorVersion = match.group(3)
        result.sparkPatchVersion = match.group(4)
        result.suffix = match.group(5)
        return result
