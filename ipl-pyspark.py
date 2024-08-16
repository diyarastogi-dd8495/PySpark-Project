# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import pandas as pd

%matplotlib inline
spark = SparkSession.builder.appName("IPL PROJECT").getOrCreate()

# COMMAND ----------

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

ball_by_ball_df = spark.read.schema(ball_by_ball_schema).format("csv").option("header", "true").option("inferSchema","true").load("s3://ipl-tvsm-mini-project/Ball_By_Ball.csv")

# ball_by_ball_df.show(5)

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  # YearType is not directly available, using IntegerType for year
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

match_df = spark.read.schema(match_schema).format("csv").option("header", "true").option("inferSchema","true").load("s3://ipl-tvsm-mini-project/Match.csv")

# match_df.show(5)

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(10, 2), True),  
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])


# COMMAND ----------

player_match_df = spark.read.schema(player_match_schema).format("csv").option("header", "true").option("inferSchema","true").load("s3://ipl-tvsm-mini-project/Player_match.csv")

# player_match_df.show(5)

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

player_df = spark.read.schema(player_schema).format("csv").option("header", "true").option("inferSchema","true").load("s3://ipl-tvsm-mini-project/Player.csv")

# player_df.show(5)

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

# COMMAND ----------

team_df = spark.read.schema(team_schema).format("csv").option("header", "true").option("inferSchema","true").load("s3://ipl-tvsm-mini-project/Team.csv")

# team_df.show(5)

# COMMAND ----------

### TRANSFORMATIONS ###

# COMMAND ----------

# exclude no balls and wides (show only valid balls)
ball_by_ball_df = ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))

# ball_by_ball_df.show(5)

# COMMAND ----------

# calculating total and average runs scored in each match and inning

total_and_avg_df = ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("avg_runs")
)

# total_and_avg_df.show(5)

# COMMAND ----------

# calculating total runs in each match for each over

windowSpec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")

ball_by_ball_df = ball_by_ball_df.withColumn("running_total_runs", sum("runs_scored").over(windowSpec))

# ball_by_ball_df.show(5)

# COMMAND ----------

# conditional: show high imact balls (either a wicket or more than 6 runs including extras)

ball_by_ball_df = ball_by_ball_df.withColumn(
    "high_impact", when(((col("runs_scored")) + (col("extra_runs")) > 6) | (col("bowler_wicket") == True), True).otherwise(False)
)

# ball_by_ball_df.show(5)

# COMMAND ----------

match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("month", month("match_date"))
match_df = match_df.withColumn("day", dayofmonth("match_date"))

# COMMAND ----------

# categorize margins into high, medium and low

match_df = match_df.withColumn(
    "win_margin_category", when((col("win_margin") >= 100), "High").when((col("win_margin") < 100) & (col("win_margin") >= 50), "Medium").otherwise("Low")
)

# match_df.show(1)

# COMMAND ----------

# analyzing relation between winning the toss and winning the match

match_df = match_df.withColumn(
    "toss_match_winner", when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
)
# match_df.show(1)

# toss_match_win_df = match_df.withColumn(
#     "toss_match_win_probability", f.expr(r"regexp_count(toss_match_winner, 'Yes')")
# )

toss_match_win_df = match_df.groupBy("match_id", "season_year", "toss_match_winner").agg(
    f.expr(r"regexp_count(toss_match_winner, 'Yes')").alias("toss_match_win_num"),
    f.expr(r"regexp_count(toss_match_winner, 'No')").alias("toss_match_lose_num"),
)

toss_match_win_prob_df = toss_match_win_df.groupBy("season_year").agg(
    sum("toss_match_win_num").alias("toss_match_win_total"),
    sum("toss_match_lose_num").alias("toss_match_lose_total")
)

# toss_match_win_df.show(5)
# toss_match_win_prob_df.show(5)

# COMMAND ----------

# normalizing and cleaning player names
player_df = player_df.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))

# handling missing values
player_df = player_df.na.fill({"batting_hand":"unknown", "bowling_skill":"unknown"})

# categorizing based on batting hand
player_df = player_df.withColumn(
    "batting_style", when((col("batting_hand").contains("Left")), "Left-Handed").otherwise("Right-Handed")
)

# player_df.show(5)

# COMMAND ----------

# check if player is veteran or not
player_match_df = player_match_df.withColumn(
    "veteran_status", when(col("age_as_on_match")>35, "Veteran").otherwise("Non-Veteran")
)

# player_match_df = player_match_df.filter(col("batting_status") != "Did Not Bat")

# calculate years since match (dynamic)
player_match_df = player_match_df.withColumn(
    "years_since_match", year(current_date()) - col("season_year")
)

# player_match_df.show(5)

# COMMAND ----------

# convert csv to sql table as a view

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_match_df.createOrReplaceTempView("player_match")
player_df.createOrReplaceTempView("player")
team_df.createOrReplaceTempView("team")


# COMMAND ----------

top_scoring_batsmen_per_szn = spark.sql("""
SELECT p.player_name, m.season_year, SUM(b.runs_scored) AS total_runs
FROM ball_by_ball b
JOIN match m on b.match_id = m.match_id
JOIN player_match pm on m.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p on p.player_id = pm.player_id
GROUP BY p.player_name, m.season_year
ORDER BY m.season_year, total_runs DESC
""")

# top_scoring_batsmen_per_szn.show()

# COMMAND ----------

economical_bowlers_powerplay = spark.sql("""
SELECT 
p.player_name, 
AVG(b.runs_scored) AS avg_runs_per_ball, 
COUNT(b.bowler_wicket) AS total_wickets
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
JOIN player p ON pm.player_id = p.player_id
WHERE b.over_id <= 6
GROUP BY p.player_name
HAVING COUNT(*) >= 1
ORDER BY avg_runs_per_ball, total_wickets DESC
""")

# economical_bowlers_powerplay.show()

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
       CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM match m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
""")

# toss_impact_individual_matches.show()

# COMMAND ----------

average_runs_in_wins = spark.sql("""
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins ASC
""")

# average_runs_in_wins.show()

# COMMAND ----------

### PLOTTING GRAPHS ###

# COMMAND ----------

economical_bowlers_plot = economical_bowlers_powerplay.toPandas()

plt.figure(figsize(8,8))

top_economical_bowlers = economical_bowlers_plot.nsmallest(10, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='skyblue')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------


