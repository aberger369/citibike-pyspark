import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as f

def create_dataframe(filepath, spark):
    df = spark.read.option("header", True).csv(filepath)
    print(df.columns)
    return df


def analysis(df):
    #create col for leisure (same start and end station)
    df2 = df.withColumn('Leisure', when(df['Start Station ID'] == df['End Station ID'], 1)\
        .when(df['Start Station ID'] != df['End Station ID'], 0))
    #pct_leisure =((df2['Leisure'] == 1).sum()/(df2['Leisure']==0).sum())*100
    print("percent of trips that start and end at same spot:")
    df2.agg({'Leisure': 'mean'}).show()
    pass



if __name__ == '__main__':

    filename = sys.argv[1]
    #save_output = sys.argv[2]


    # Start spark session
    spark = SparkSession.builder.getOrCreate()

    df = create_dataframe(filename, spark)
    analysis(df)

    # output_df.write.csv(save_output, mode='overwrite', header=True)   
    
    # Stop spark session
    spark.stop()

