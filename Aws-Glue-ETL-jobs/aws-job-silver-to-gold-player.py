import sys
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, countDistinct, row_number
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer desde catálogo Glue
df_games_player = glueContext.create_dynamic_frame.from_catalog(
    database="db_silver",
    table_name="sv_player_gamesscore"
)

# Convertir a DataFrame de Spark
df_games_player = df_games_player.toDF()

# -------------------------------
# OBTENER ÚLTIMO EQUIPO POR JUGADOR
# -------------------------------

# Convertir game_date a tipo fecha si es necesario
df_games_player = df_games_player.withColumn("game_date", col("game_date").cast("date"))

# Crear ventana ordenada por game_date descendente por jugador
window_latest = Window.partitionBy("player_name").orderBy(col("game_date").desc())

# Obtener team_name y team_abbr del último equipo para cada jugador
df_latest_team = df_games_player.withColumn("row_num", row_number().over(window_latest)) \
    .filter(col("row_num") == 1) \
    .select(
        "player_name",
        col("team_name").alias("team_name_max"),
        col("team_abbr").alias("team_abbr_max")
    )

# -------------------------------
# AGRUPAR MÉTRICAS POR JUGADOR
# -------------------------------

grouped_df = df_games_player.groupBy(
    "player_name", "player_position", "height", "weight",
    "country", "birthdate", "draft_year", "draft_number", "image_name"
).agg(
    sum("field_goals_made").alias("field_goals_made"),
    sum("field_goals_attempted").alias("field_goals_attempted"),
    sum("three_point_field_goals_made").alias("three_point_field_goals_made"),
    sum("three_point_field_goals_attempted").alias("three_point_field_goals_attempted"),
    sum("free_throws_made").alias("free_throws_made"),
    sum("free_throws_attempted").alias("free_throws_attempted"),
    sum("offensive_rebounds").alias("offensive_rebounds"),
    sum("defensive_rebounds").alias("defensive_rebounds"),
    sum("assists").alias("assists"),
    sum("steals").alias("steals"),
    sum("blocks").alias("blocks"),
    sum("turnovers").alias("turnovers"),
    sum("personal_fouls").alias("personal_fouls"),
    sum("points").alias("points"),
    ((sum("seconds_played") + sum("minutes_to_seconds")) / 60).cast(DoubleType()).alias("total_minutes_played"),
    countDistinct("game_date").alias("games_played")
)

# Unir métricas con último equipo
df_player = grouped_df.join(df_latest_team, on="player_name", how="left")

# -------------------------------
# FUNCIÓN PARA ESCRIBIR A S3
# -------------------------------

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
            new_key = f"gold/{file_name}/{file_name}.parquet"
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
            s3.delete_object(Bucket=bucket, Key=key)
            break
    
    # Limpiar carpeta temporal
    for obj in response.get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])

# -------------------------------
# GUARDAR RESULTADO EN GOLD
# -------------------------------

write_df_with_custom_name(df_player, "player_resume")

# Finalizar trabajo
job.commit()