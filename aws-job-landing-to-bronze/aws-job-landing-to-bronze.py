import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, split, to_date, when, date_format
from pyspark.sql.types import LongType, IntegerType

import uuid

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer desde catalogo Glue
dyf_gamelogs = glueContext.create_dynamic_frame.from_catalog(
    database="db_landing",
    table_name="ld_gamelogs"
)
dyf_games = glueContext.create_dynamic_frame.from_catalog(
    database="db_landing",
    table_name="ld_games"
)
dyf_season = glueContext.create_dynamic_frame.from_catalog(
    database="db_landing",
    table_name="ld_season"
)
dyf_teams = glueContext.create_dynamic_frame.from_catalog(
    database="db_landing",
    table_name="ld_teams"
)

# Convertir a DataFrame
df_gamelogs = dyf_gamelogs.toDF()
df_games = dyf_games.toDF()
df_season = dyf_season.toDF()
df_teams = dyf_teams.toDF()

# Procesamiento de gamelogs
df_gamelogs = df_gamelogs.withColumn("game_date", date_format(to_date("game_date", "MMM dd, yyyy"), "yyyy-MM-dd"))
df_gamelogs = df_gamelogs.drop("partition_0","fg_pct","fg3_pct","ft_pct","reb")

# procesamiento de games
df_games = df_games.withColumn("min", split(col("minutos"), ":")[0].cast("int"))
df_games = df_games.withColumn("seg", split(col("minutos"), ":")[1].cast("int"))
df_games = df_games.drop("minutos","partition_0","fg%","3p%","trb","gmsc","ft%")

# Procesamiento de season
df_season = df_season.drop("player_id", "season_id","league_id","team_id","team_abbreviation","player_age"
                           ,"gp","gs","min","fgm","fga","fg_pct","fg3m","fg3a","fg3_pct","ftm","fta","ft_pct"
                           ,"oreb","dreb","reb","ast","stl","blk","tov","pf","pts","partition_0"
                           )
df_season = df_season.dropDuplicates()
df_season = df_season.withColumn("birthdate",when(col("birthdate").isNotNull(), to_date(col("birthdate"), "yyyy-MM-dd'T'HH:mm:ss")))


# Procesamiento de teams
df_teams = df_teams.drop("partition_0")


# Funcion para cambiar tipo de dato bigint a int
def cast_bigint_to_int_if_safe(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, LongType):
            col_name = field.name
            try:
                # Revisa si todos los valores son menores a 2^31 (maximo valor de int)
                max_value = df.select(col_name).rdd.map(lambda r: r[0]).filter(lambda x: x is not None).max()
                if max_value <= 2147483647:
                    print(f"Casteando '{col_name}' de bigint a int")
                    df = df.withColumn(col_name, col(col_name).cast("int"))
                else:
                    print(f"Saltando '{col_name}' (tiene valor mayor a INT)")
            except Exception as e:
                print(f"Error revisando '{col_name}': {e}")
    return df

# Funcion para escribir un DataFrame con nombre personalizado
def write_df_with_custom_name(df, file_name):
    df = df.coalesce(1)
    tmp_path = f"s3://datalake-nba-dmc/tmp_output_{str(uuid.uuid4())}"
    df.write.mode("overwrite").parquet(tmp_path)
    
    s3 = boto3.client("s3")
    bucket = "datalake-nba-dmc"
    prefix = tmp_path.replace("s3://datalake-nba-dmc/", "")
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".parquet"):
            new_key = f"bronze/{file_name}/{file_name}.parquet"
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
            s3.delete_object(Bucket=bucket, Key=key)
            break
    
    # Limpiar carpeta temporal
    for obj in response.get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
        
# Cambio de tipo de dato
df_gamelogs = cast_bigint_to_int_if_safe(df_gamelogs)
df_games = cast_bigint_to_int_if_safe(df_games)
df_season = cast_bigint_to_int_if_safe(df_season)
df_teams = cast_bigint_to_int_if_safe(df_teams)

# Escribir ambos archivos con nombre personalizado
write_df_with_custom_name(df_gamelogs, "gamelogs")
write_df_with_custom_name(df_games, "games")
write_df_with_custom_name(df_season, "season")
write_df_with_custom_name(df_teams, "teams")


job.commit()