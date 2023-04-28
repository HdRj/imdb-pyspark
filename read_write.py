def read(path, schema, spark_session):
    return spark_session.read.option("header", True).csv(path, schema, '\t')



def write(df, directory_to_write):
    df.write.csv(directory_to_write, header=True)
    return