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
dyf_gamelogs = glueContext.create_dynamic_frame.from_catalog(
    database="db_bronze",
    table_name="bz_gamelogs"
)
dyf_games = glueContext.create_dynamic_frame.from_catalog(
    database="db_bronze",
    table_name="bz_games"
)
dyf_season = glueContext.create_dynamic_frame.from_catalog(
    database="db_bronze",
    table_name="bz_season"
)
dyf_teams = glueContext.create_dynamic_frame.from_catalog(
    database="db_bronze",
    table_name="bz_teams"
)


# Convertir a DataFrame
df_gamelogs = dyf_gamelogs.toDF()
df_games = dyf_games.toDF()
df_season = dyf_season.toDF()
df_teams = dyf_teams.toDF()


# Modificaciones de gamelogs
df_gamelogs = (
    df_gamelogs
    .withColumnRenamed("wl","game_result")
    .withColumnRenamed("fgm","field_goals_made")
    .withColumnRenamed("fga","field_goals_attempted")
    .withColumnRenamed("fg3m","three_point_field_goals_made")
    .withColumnRenamed("fg3a","three_point_field_goals_attempted")
    .withColumnRenamed("ftm","free_throws_made")
    .withColumnRenamed("fta","free_throws_attempted")
    .withColumnRenamed("oreb","offensive_rebounds")
    .withColumnRenamed("dreb","defensive_rebounds")
    .withColumnRenamed("ast","assists")
    .withColumnRenamed("stl","steals")
    .withColumnRenamed("blk","blocks")
    .withColumnRenamed("tov","turnovers")
    .withColumnRenamed("pf","personal_fouls")
    .withColumnRenamed("pts","points")
)

# Modificaciones de games
df_games = (
    df_games
    .withColumnRenamed("fecha", "game_date")
    .withColumnRenamed("equipo", "team_name")
    .withColumnRenamed("cuarto", "quarter")
    .withColumnRenamed("jugador", "player_name")
    .withColumnRenamed("titular/suplente", "player_role")    
    .withColumnRenamed("fg","field_goals_made")
    .withColumnRenamed("fga","field_goals_attempted")
    .withColumnRenamed("3p","three_point_field_goals_made")
    .withColumnRenamed("3pa","three_point_field_goals_attempted")
    .withColumnRenamed("ft","free_throws_made")
    .withColumnRenamed("fta","free_throws_attempted")
    .withColumnRenamed("orb","offensive_rebounds")
    .withColumnRenamed("drb","defensive_rebounds")
    .withColumnRenamed("ast","assists")
    .withColumnRenamed("stl","steals")
    .withColumnRenamed("blk","blocks")
    .withColumnRenamed("tov","turnovers")
    .withColumnRenamed("pf","personal_fouls")
    .withColumnRenamed("pts","points")
    .withColumnRenamed("+/-","plus_minus")
    .withColumnRenamed("min","minutes_played")
    .withColumnRenamed("seg","seconds_played")
    .withColumn("minutes_to_seconds",col("minutes_played")*60)
)

# Modificaciones de season
df_season = (
    df_season
    .withColumnRenamed("position","player_position")
)

# Modificaciones de teams
df_teams = (
    df_teams
    .withColumnRenamed("team","team_name")
    .withColumnRenamed("nametag","team_abbr")
    .withColumnRenamed("year","season_year")
    .withColumnRenamed("classification","league_classification")
)


# Join de la tabla gamelogs_season
df_join_gamelogs_season = df_gamelogs.alias("gl").join(
    df_season.alias("ss"),
    on=(df_gamelogs["player_name"] == df_season["player_name"])
    ,how="left"
)


df_logs_gamesseason = df_join_gamelogs_season.select(
    col("season_id"),
    col("player_id"),
    col("game_id"),
    col("game_date"),
    col("matchup"),
    col("game_result"),
    col("min"),
    col("field_goals_made"),
    col("field_goals_attempted"),
    col("three_point_field_goals_made"),
    col("three_point_field_goals_attempted"),
    col("free_throws_made"),
    col("free_throws_attempted"),
    col("offensive_rebounds"),
    col("defensive_rebounds"),
    col("assists"),
    col("steals"),
    col("blocks"),
    col("turnovers"),
    col("personal_fouls"),
    col("points"),
    col("plus_minus"),
    col("video_available"),
    col("gl.player_name"),
    col("gl.image_name"),
    col("ss.team_name_current"),
    col("ss.team_city"),
    col("ss.player_position"),
    col("ss.height"),
    col("ss.weight"),
    col("ss.country"),
    col("ss.birthdate"),
    col("ss.draft_year"),
    col("ss.draft_number"),
    col("ss.data_type"),
    col("ss.partition_1")
)


# Join de la tabla games_season_teams
df_join_games_teams = df_games.alias("g").join(
    df_teams.alias("t"),
    on=(
        (col("g.team_name") == col("t.team_name")) &
        (year(to_date(col("g.game_date"))) == col("t.season_year"))
    ),
    how="left"
)
df_join_games_teams = df_join_games_teams.select(
    col("g.game_date"),
    col("g.team_name"),
    col("g.quarter"),
    col("g.player_name"),
    col("g.player_role"),
    col("g.field_goals_made"),
    col("g.field_goals_attempted"),
    col("g.three_point_field_goals_made"),
    col("g.three_point_field_goals_attempted"),
    col("g.free_throws_made"),
    col("g.free_throws_attempted"),
    col("g.offensive_rebounds"),
    col("g.defensive_rebounds"),
    col("g.assists"),
    col("g.steals"),
    col("g.blocks"),
    col("g.turnovers"),
    col("g.personal_fouls"),
    col("g.points"),
    col("g.plus_minus"),
    col("g.partition_1"),
    col("g.minutes_played"),
    col("g.seconds_played"),
    col("g.minutes_to_seconds"),
    col("t.team_abbr"),
    col("t.division"),
    col("t.conference"),
    col("t.season_year"),
    col("t.league_classification")
)

# Segundo join: LEFT JOIN df_season
df_season = df_season.withColumn("team_full_name",concat_ws(" ", col("team_city"), col("team_name_current")))

df_join_games_season_teams = df_join_games_teams.alias("g").join(
    df_season.alias("s"),
    on=(
        (col("g.player_name") == col("s.player_name")) &
        (col("g.team_name") == col("s.team_full_name"))
        ),
    how="left"
)

df_games_season_teams = df_join_games_season_teams.select(
    col("g.game_date"),
    col("g.team_name"),
    col("g.quarter"),
    col("g.player_name"),
    col("g.player_role"),
    col("g.field_goals_made"),
    col("g.field_goals_attempted"),
    col("g.three_point_field_goals_made"),
    col("g.three_point_field_goals_attempted"),
    col("g.free_throws_made"),
    col("g.free_throws_attempted"),
    col("g.offensive_rebounds"),
    col("g.defensive_rebounds"),
    col("g.assists"),
    col("g.steals"),
    col("g.blocks"),
    col("g.turnovers"),
    col("g.personal_fouls"),
    col("g.points"),
    col("g.plus_minus"),
    col("g.partition_1"),
    col("g.minutes_played"),
    col("g.seconds_played"),
    col("g.minutes_to_seconds"),
    col("g.team_abbr"),
    col("g.division"),
    col("g.conference"),
    col("g.season_year"),
    col("g.league_classification"),
    col("s.team_name_current"),
    col("s.team_city"),
    col("s.player_position"),
    col("s.height"),
    col("s.weight"),
    col("s.country"),
    col("s.birthdate"),
    col("s.draft_year"),
    col("s.draft_number"),
    col("s.image_name"),
    col("s.data_type")

)



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
write_df_with_custom_name(df_logs_gamesseason, "logs_gamesseason")
write_df_with_custom_name(df_games_season_teams,"games_season_teams")

job.commit()