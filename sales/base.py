from pyspark.sql import SparkSession
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s : %(levelname)s :%(name)s :%(message)s',
    datefmt="%m/%d/%Y %I:%H:%S %p",
    filename="logfile.log",
    filemode="w",
    level=logging.INFO
)


class Utility:

    def __init__(self):
        logger.info("Creating spark session from utility class")
        self.spark = SparkSession.builder.appName("POC1").master("local").getOrCreate()
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    def readfile(self, filename, delimit=",", schema=None):
        """
        read a csv file in a dataframe
        :param delimit: custom delimiter can be provided
        :param filename: file which needs to be read
        :param schema: custom schema for the file, none by default
        :return: df
        """
        logger.info("Reading %s file into dataframe", filename)
        try:
            if schema:
                df = self.spark.read.\
                    option("delimiter", delimit).\
                    csv(filename, header=True, schema=schema)
            else:
                df = self.spark.read. \
                    option("delimiter",delimit). \
                    csv(filename, header=True, inferSchema=True)
        except Exception as e:
            logger.exception("Unable to read file Exception %s occurred", e)
            print("Unable to save file due to exception %s. ", e)
        else:
            return df

    def writefile(self, df, filename):
        """
        Write dataframe to csv file with the given filename
        :param df: dataframe to be written
        :param filename: filename in which dataframe is written
        :return: None
        """
        logger.info("Writing dataframe to file %s", filename)
        try:
            df.write.mode("overwrite").csv(filename)
        except Exception as e:
            logger.exception("Unable to save file Exception %s occurred", e)
            print("Unable to save file due to",e)

