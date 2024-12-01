from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, split, when, substring, lit
import pandas as pd

conf = SparkConf().setAppName("ChessGames").setMaster("local[*]")
sc = SparkContext(conf=conf)

spark = SparkSession(sc)

file_path_saved_games = '/scratch/zrc3hc/combined_games.csv'
combined_games_df = spark.read.csv(file_path_saved_games, header=True, inferSchema=True)


combined_games_df.select("game_id").distinct().count()

initial_positions = {
    "Move": 0,
    "White_Rook_1": "a1",
    "White_Rook_2": "h1",
    "White_Knight_1": "b1",
    "White_Knight_2": "g1",
    "White_Bishop_1": "c1",
    "White_Bishop_2": "f1",
    "White_Queen_1": "d1",
    "White_King_1": "e1",
    "White_Pawn_1": "a2",
    "White_Pawn_2": "b2",
    "White_Pawn_3": "c2",
    "White_Pawn_4": "d2",
    "White_Pawn_5": "e2",
    "White_Pawn_6": "f2",
    "White_Pawn_7": "g2",
    "White_Pawn_8": "h2",
    "Black_Rook_1": "a8",
    "Black_Rook_2": "h8",
    "Black_Knight_1": "b8",
    "Black_Knight_2": "g8",
    "Black_Bishop_1": "c8",
    "Black_Bishop_2": "f8",
    "Black_Queen_1": "d8",
    "Black_King_1": "e8",
    "Black_Pawn_1": "a7",
    "Black_Pawn_2": "b7",
    "Black_Pawn_3": "c7",
    "Black_Pawn_4": "d7",
    "Black_Pawn_5": "e7",
    "Black_Pawn_6": "f7",
    "Black_Pawn_7": "g7",
    "Black_Pawn_8": "h7",
}
df = pd.DataFrame([initial_positions])


schema = StructType([
    StructField("Move", IntegerType(), True),
    StructField("White_Rook_1", StringType(), True),
    StructField("White_Rook_2", StringType(), True),
    StructField("White_Knight_1", StringType(), True),
    StructField("White_Knight_2", StringType(), True),
    StructField("White_Bishop_1", StringType(), True),
    StructField("White_Bishop_2", StringType(), True),
    StructField("White_Queen_1", StringType(), True),
    StructField("White_King_1", StringType(), True),
    StructField("White_Pawn_1", StringType(), True),
    StructField("White_Pawn_2", StringType(), True),
    StructField("White_Pawn_3", StringType(), True),
    StructField("White_Pawn_4", StringType(), True),
    StructField("White_Pawn_5", StringType(), True),
    StructField("White_Pawn_6", StringType(), True),
    StructField("White_Pawn_7", StringType(), True),
    StructField("White_Pawn_8", StringType(), True),
    StructField("Black_Rook_1", StringType(), True),
    StructField("Black_Rook_2", StringType(), True),
    StructField("Black_Knight_1", StringType(), True),
    StructField("Black_Knight_2", StringType(), True),
    StructField("Black_Bishop_1", StringType(), True),
    StructField("Black_Bishop_2", StringType(), True),
    StructField("Black_Queen_1", StringType(), True),
    StructField("Black_King_1", StringType(), True),
    StructField("Black_Pawn_1", StringType(), True),
    StructField("Black_Pawn_2", StringType(), True),
    StructField("Black_Pawn_3", StringType(), True),
    StructField("Black_Pawn_4", StringType(), True),
    StructField("Black_Pawn_5", StringType(), True),
    StructField("Black_Pawn_6", StringType(), True),
    StructField("Black_Pawn_7", StringType(), True),
    StructField("Black_Pawn_8", StringType(), True),
])

schema1 = StructType([
    StructField("Move", IntegerType(), True),
    StructField("White_Rook_1", StringType(), True),
    StructField("White_Rook_2", StringType(), True),
    StructField("White_Knight_1", StringType(), True),
    StructField("White_Knight_2", StringType(), True),
    StructField("White_Bishop_1", StringType(), True),
    StructField("White_Bishop_2", StringType(), True),
    StructField("White_Queen_1", StringType(), True),
    StructField("White_King_1", StringType(), True),
    StructField("White_Pawn_1", StringType(), True),
    StructField("White_Pawn_2", StringType(), True),
    StructField("White_Pawn_3", StringType(), True),
    StructField("White_Pawn_4", StringType(), True),
    StructField("White_Pawn_5", StringType(), True),
    StructField("White_Pawn_6", StringType(), True),
    StructField("White_Pawn_7", StringType(), True),
    StructField("White_Pawn_8", StringType(), True),
    StructField("Black_Rook_1", StringType(), True),
    StructField("Black_Rook_2", StringType(), True),
    StructField("Black_Knight_1", StringType(), True),
    StructField("Black_Knight_2", StringType(), True),
    StructField("Black_Bishop_1", StringType(), True),
    StructField("Black_Bishop_2", StringType(), True),
    StructField("Black_Queen_1", StringType(), True),
    StructField("Black_King_1", StringType(), True),
    StructField("Black_Pawn_1", StringType(), True),
    StructField("Black_Pawn_2", StringType(), True),
    StructField("Black_Pawn_3", StringType(), True),
    StructField("Black_Pawn_4", StringType(), True),
    StructField("Black_Pawn_5", StringType(), True),
    StructField("Black_Pawn_6", StringType(), True),
    StructField("Black_Pawn_7", StringType(), True),
    StructField("Black_Pawn_8", StringType(), True),
    StructField("result", IntegerType(), True),
])

df = spark.createDataFrame(df, schema=schema)



#game_ids = combined_games_df.select("game_id").distinct().collect()
game_ids = [row["game_id"] for row in combined_games_df.select("game_id").distinct().collect()]

df_combined = spark.createDataFrame([], schema1)

count = 0

# looping over all unique game ids
for game_id in game_ids:
    
    game_id_count = 0 
    
    # Step 1. merge information 
    
    current_game_info = combined_games_df.filter(combined_games_df.game_id == game_id)
    
    df_current = df.join(current_game_info, on="Move", how="left")
    
    # removing duplicate columns
    
    df_current = df_current.select(*(col for col in df_current.columns if df_current.columns.count(col) == 1))
    
    total_moves = current_game_info.count()
    
    filtered_df = combined_games_df.filter(combined_games_df.game_id == game_ids[game_id]).select("Move", "next_move")

    for move in range(0, total_moves): 
        
        # Step 2: Duplicate the previous row

        previous_row = df_current.filter(col("Move") == move - 1)
        new_row = previous_row.withColumn("Move", col("Move") + 1)
        
        # Step 3: Split the 'next_move' column into 'from_square' and 'to_square'
    
        new_row = new_row.withColumn("from_square", substring(col("next_move"), 1, 2)) \
                         .withColumn("to_square", substring(col("next_move"), 3, 2))

        # Step 4a: Check if `to_square` is already present in the new row

        # If it is, it means a piece was captured, so update the piece at `from_square` to `0`

        columns_to_check = [c for c in df_current.columns if c not in ["Move", "next_move", "from_square", "to_square"]]
        for column in columns_to_check:
            new_row = new_row.withColumn(
                column,
                when(
                    (col("to_square") == col(column)), 
                    "0"  # Set the piece at `to_square` to `0` to indicate it was captured
                ).otherwise(col(column)))  # retain the original value otherwise
    


        # Step 4b: Update the piece positions for the new row
        
        for column in columns_to_check:
            new_row = new_row.withColumn(
                column,
                when(
                    (col("Move") == move) & (col(column) == col("from_square")),  # Match `from_square`
                    col("to_square")  # Update to `to_square`
                ).otherwise(col(column)))  # Retain the original value otherwise


        # Step 5: Update the 'Next Move' by mapping

        if game_id_count == 0:
            df_current = df_current.withColumn("game_id", lit(game_id))
            game_id_count += 1
            
        
        new_row = new_row.select(*(col for col in new_row.columns if new_row.columns.count(col) == 1))

        new_row = new_row.drop("from_square", "to_square")
        
        df_current = df_current.union(new_row)
        
        df_current = df_current.drop("next_move")
        
        df_current = df_current.join(filtered_df, on="Move", how="left")

        print(f"Game ID: {game_id}, Move {move} processed...")
        
    
    if count == 0:
        
        df_combined = df_combined.withColumn("game_id", lit(game_id))
        df_combined = df_combined.withColumn("result", lit(0))
        df_combined = df_combined.withColumn("next_move", lit(0))
        count += 1
    
    df_combined = df_combined.union(df_current)

        

output_path = "/scratch/zrc3hc/df_combined_output"
df_combined.repartition(1).write.csv(output_path, header=True, mode="overwrite")

