import os
import sys
from pyspark.sql import SparkSession



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.appName('test').getOrCreate()
    data = [("James", "", "Smith", "36636", "M", 60000),
            ("Michael", "Rose", "", "40288", "M", 70000),
            ("Robert", "", "Williams", "42114", "", 400000),
            ("Maria", "Anne", "Jones", "39192", "F", 500000),
            ("Jen", "Mary", "Brown", "", "F", 0)]

    columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
    pysparkDF = spark.createDataFrame(data=data, schema=columns)
    pysparkDF.printSchema()
    pysparkDF.show(truncate=False)
    pandas_df = pysparkDF.toPandas()
    print(pandas_df)
    pandas_df.to_csv(r'Z:\test.csv', index=None, header=True)
    print("Score key Saved Successfully")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
