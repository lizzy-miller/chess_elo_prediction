## Project Progress and Next Steps

1. Filtering Games from BGN File
- Started with a filtered PGN file (filtered_bullet_games_first_1000.pgn) that contains only "Rated Bullet games" games. 
- Each game in this file was parsed using the chess.pgn library to extract its moves and relevant metadata

2. Saved Files
I have saved two files to store the extracted data. 

**converted_games_data.parquet**

- This file contains the game data in a format suitable for distributed processing. 
- for each move in each game, the board position was converted to an 8 x 8 x 12 matrix to represent the rpescense of each piece type of the chessboard. 

**filtered_games_summary.csv**
- This file contains the metadata about each game. 
- For each game, this file contains the game_id, WhiteElo, BlackElo, WhiteRatingDiff, BlackRatingDiff, and Result. 

The filepaths for both data are below: 

parquet_file_path = '/scratch/zrc3hc/converted_games_data.parquet'
output_csv_path = '/scratch/zrc3hc/filtered_games_summary.csv'
