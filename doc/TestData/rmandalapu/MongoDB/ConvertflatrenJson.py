from pyspark.sql import functions as F
from pyspark.sql.types import *

# Converting struct datatype to string datatype
def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

# passing the array type schema to array_to_string_udf function
def expArrayDF(df):
    for (name, dtype) in df.dtypes:
        if "array" in dtype:
            df = df.withColumn(name, F.explode(name))

    return df

# identifying the array/struct data types

def df_is_flat(df):
    for (_, dtype) in df.dtypes:
        if ("array" in dtype) or ("struct" in dtype):
            return False

    return True


#creating the loop for to iterate the schema into convertion functions
def flatJson(jdf):
    keepGoing = True
    while(keepGoing):
        fields = flatten(jdf.schema)
        new_fields = [item.replace(".", "_") for item in fields]
        jdf = jdf.select(fields).toDF(*new_fields)
        jdf = expArrayDF(jdf)
        if df_is_flat(jdf):
            keepGoing = False

    return jdf