import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, to_date, sum, concat_ws
from pyspark.sql.types import NumericType
import uuid

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer desde catalogo Glue
dyf_games_season_teams = glueContext.create_dynamic_frame.from_catalog(
    database="db_silver",
    table_name="sv_games_season_teams"
)


# Convertir a DataFrame
df_games_season_teams = dyf_games_season_teams.toDF()

# Creación de la tabla player

df_player = df_games_season_teams.drop("division","conference"
                                       ,"season_year","league_classification"
                                       ,"team_name_current","team_city","data_type")

# Creación de la tabla team
df_team = df_games_season_teams.drop("quarter","player_role","player_name"
                                     ,"plus_minus","minutes_played","seconds_played"
                                     ,"minutes_to_seconds","player_position","height"
                                     ,"weight","country","birthdate","draft_year","draft_number"
                                     ,"image_name","data_type","team_name_current","team_city")

group_cols_team = ["game_date", "team_name","partition_1","team_abbr","division"
                   ,"conference","season_year","league_classification"]

agg_cols_team  = [c for c, t in df_team.dtypes if t in ('int') and c not in group_cols_team]

agg_exprs_fteam = [sum(col(c)).alias(c) for c in agg_cols_team]

df_teams = df_team.groupBy(*group_cols_team).agg(*agg_exprs_fteam)


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
            new_key = f"silver/{file_name}/{file_name}.parquet"
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
            s3.delete_object(Bucket=bucket, Key=key)
            break
    
    # Limpiar carpeta temporal
    for obj in response.get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])

# Escribir ambos archivos con nombre personalizado
write_df_with_custom_name(df_player, "player_gamesscore")
write_df_with_custom_name(df_teams,"teams_gamesscore")

job.commit()