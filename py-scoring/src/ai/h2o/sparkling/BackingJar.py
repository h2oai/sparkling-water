
class BackingJar(object):

    @staticmethod
    def getName():
        return "sparkling_water_scoring_assembly.jar"

    @staticmethod
    def getRelativePath():
        return "sparkling_water/" + BackingJar.getName()
