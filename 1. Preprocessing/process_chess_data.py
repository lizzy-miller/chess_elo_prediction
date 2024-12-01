import chess
import chess.pgn
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, split, when, substring, lit
import pandas as pd


spark = SparkSession.builder.appName("ChessGames").getOrCreate()

# Starting by first converting starting the pieces at their initial positions.
# Every game begins like this

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


# Saving and or upload combined games data...
file_path_saved_games = '/scratch/zrc3hc/combined_games.csv'
#combined_games_df.write.csv(file_path, header=True, mode="overwrite")
combined_games_df = spark.read.csv(file_path_saved_games, header=True, inferSchema=True)



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
    StructField("game_id", StringType(), True),
    StructField("next_move", StringType(), True),
    StructField("result", IntegerType(), True),
])

# Blank canvas for each game
df = pd.DataFrame([initial_positions])

df = spark.createDataFrame(df, schema=schema)

# Step 1. First, calculate which rows we will be saving (only the last 10 percent of each game...)

saved_games = df_combined = spark.createDataFrame([], schema1)
    
current_game_info = combined_games_df.filter(combined_games_df.game_id == 29)

next_moves = [row["next_move"] for row in current_game_info.select("next_move").collect()]

total_moves = current_game_info.count()

number_of_moves = int(total_moves * .1)

moves_captured_range = range(total_moves - number_of_moves, total_moves)

# Step 2. Denote the previous row

current_game = df.join(current_game_info, on="Move", how="left")

# removing duplicate columns

current_game = current_game.select(*(col for col in current_game.columns if current_game.columns.count(col) == 1))
move_count = 0

previous_row = current_game.filter(col("Move") == move_count)

next_moves = [row["next_move"] for row in current_game_info.select("next_move").collect()]

row_list = []

for move in range(0, total_moves - 1): 
    
    game_id = 29
        
    # Step 2: Duplicate the previous row

    new_row = previous_row.withColumn("Move", col("Move") + 1)
    

    # Step 3: Split the 'next_move' column into 'from_square' and 'to_square'

    new_row = new_row.withColumn("from_square", substring(col("next_move"), 1, 2)) \
                     .withColumn("to_square", substring(col("next_move"), 3, 2))

    # Step 4a: Check if `to_square` is already present in the new row

    # If it is, it means a piece was captured, so update the piece at `from_square` to `0`

    columns_to_check = [c for c in current_game.columns if c not in ["Move", "next_move", "from_square", "to_square", "game_id", "next_move", "result"]]

    new_row = new_row.select(
        *[
        when(col("to_square") == col(column), "0").otherwise(col(column)).alias(column)
        if column in columns_to_check else col(column)
        for column in new_row.columns
        ]
    )


    # Step 4b: Update the piece positions for the new row

    new_row = new_row.select(
        *[
            when(col(column) == col("from_square"), col("to_square")).otherwise(col(column)).alias(column)
            if column in columns_to_check else col(column)
            for column in new_row.columns
        ]
    )


    move_count += 1 

    # Step 5: Update the 'Next Move' 

    new_row = new_row.withColumn("Move", lit(move_count))
    new_row = new_row.drop("from_square", "to_square")

    if move + 1 < len(next_moves):
        new_row = new_row.withColumn("next_move", lit(next_moves[move + 1]))
    else:
        new_row = new_row.withColumn("next_move", lit(None))
        print("ran out of rows")
        print(move)

    previous_row = new_row

    if move_count in moves_captured_range:
        saved_games = saved_games.union(new_row)  # Append directly to saved_games
        print("Successfully saved game")

    if move % 5 == 0:
        print(f"Game ID: {game_id}, Move {move} processed...")

if row_list:
    print()
    saved_games = spark.createDataFrame(row_list, schema=schema1)

output_path = '/sfs/gpfs/tardis/home/zrc3hc/Chess/1. Preprocessing/test_output'
saved_games.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


