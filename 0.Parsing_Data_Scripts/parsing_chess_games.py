import zstandard as zstd
import io
import chess.pgn
import json

def save_zipped_pgn_to_json_filtered(pgn_file, output_file, event_filter="Rated Blitz game", batch_size=1000000):
    parsed_game_count = 0  # Counter for how many games have been parsed
    saved_game_count = 0   # Counter for how many games have been saved (filtered)
    
    # Open the compressed input PGN file and set up Zstandard decompression
    with open(pgn_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        
        # Decompress the stream in memory
        with dctx.stream_reader(compressed_file) as reader:
            decompressed_stream = io.TextIOWrapper(reader, encoding='utf-8')
            
            # Open output file for writing the JSON data in chunks
            with open(output_file, 'wb') as output_compressed_file:
                cctx = zstd.ZstdCompressor()
                
                with cctx.stream_writer(output_compressed_file) as compressor:
                    compressor.write(b"[")  # Start of JSON array
                    
                    # Read the PGN games from the decompressed stream
                    while True:
                        game = chess.pgn.read_game(decompressed_stream)
                        if game is None:
                            break
                        
                        # Increment parsed game counter
                        parsed_game_count += 1
                        
                        # Print the parsed game count every 10,000 games
                        if parsed_game_count % 10000 == 0:
                            print(f"Parsed {parsed_game_count} games so far...")
                        
                        # Filter games by the 'Event' header
                        if game.headers.get("Event") != event_filter:
                            continue  # Skip this game if it doesn't match the event
                        
                        # Convert the game to a dictionary, selecting specific headers
                        game_info = {
                            "Event": game.headers.get("Event", ""),
                            "Site": game.headers.get("Site", ""),
                            "Date": game.headers.get("Date", ""),
                            "Result": game.headers.get("Result", ""),
                            "WhiteElo": game.headers.get("WhiteElo", ""),
                            "BlackElo": game.headers.get("BlackElo", ""),
                            "Moves": [move.uci() for move in game.mainline_moves()]  # UCI formatted moves
                        }
                        
                        # Write game to file
                        if saved_game_count > 0:
                            compressor.write(b",")  # Add comma between JSON objects
                        compressor.write(json.dumps(game_info).encode('utf-8'))
                        
                        saved_game_count += 1  # Increment saved game counter
                        
                        if saved_game_count % batch_size == 0:
                            print(f"Saved {saved_game_count} games so far...")
                    
                    compressor.write(b"]")  # End of JSON array
    
    # Print the number of parsed and saved games
    print(f"Total number of games parsed: {parsed_game_count}")
    print(f"Total number of games saved: {saved_game_count}")
    print(f"Filtered games saved to {output_file}")

# Example usage
pgn_file_path = '/sfs/gpfs/tardis/home/zrc3hc/Chess/lichess_db_standard_rated_2024-08.pgn.zst'
output_file_path = '/sfs/gpfs/tardis/home/zrc3hc/Chess/datasets/zipped_file/filtered_games.zst'

save_zipped_pgn_to_json_filtered(pgn_file_path, output_file_path, event_filter="Rated Blitz game")
import zstandard as zstd
import io
import chess.pgn
import json

def save_zipped_pgn_to_json_filtered(pgn_file, output_file_prefix, event_filter="Rated Blitz game", batch_size=1000000):
    parsed_game_count = 0  # Counter for how many games have been parsed
    saved_game_count = 0   # Counter for how many games have been saved (filtered)
    batch_count = 0        # Counter to keep track of how many batches have been saved
    
    # Open the compressed input PGN file and set up Zstandard decompression
    with open(pgn_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        
        # Decompress the stream in memory
        with dctx.stream_reader(compressed_file) as reader:
            decompressed_stream = io.TextIOWrapper(reader, encoding='utf-8')
            
            # Read the PGN games from the decompressed stream
            while True:
                # Open a new output file for each batch
                batch_count += 1
                current_output_file = f"{output_file_prefix}_batch_{batch_count}.zst"
                with open(current_output_file, 'wb') as output_compressed_file:
                    cctx = zstd.ZstdCompressor()
                    
                    with cctx.stream_writer(output_compressed_file) as compressor:
                        compressor.write(b"[")  # Start of JSON array
                        
                        current_batch_count = 0  # Track games within the current batch
                        
                        while current_batch_count < batch_size:
                            game = chess.pgn.read_game(decompressed_stream)
                            if game is None:
                                break
                            
                            # Increment parsed game counter
                            parsed_game_count += 1
                            
                            # Print the parsed game count every 10,000 games
                            if parsed_game_count % 10000 == 0:
                                print(f"Parsed {parsed_game_count} games so far...")
                            
                            # Filter games by the 'Event' header
                            if game.headers.get("Event") != event_filter:
                                continue  # Skip this game if it doesn't match the event
                            
                            # Convert the game to a dictionary, selecting specific headers
                            game_info = {
                                "Event": game.headers.get("Event", ""),
                                "Site": game.headers.get("Site", ""),
                                "Date": game.headers.get("Date", ""),
                                "Result": game.headers.get("Result", ""),
                                "WhiteElo": game.headers.get("WhiteElo", ""),
                                "BlackElo": game.headers.get("BlackElo", ""),
                                "Moves": [move.uci() for move in game.mainline_moves()]  # UCI formatted moves
                            }
                            
                            # Write game to file
                            if saved_game_count > 0 or current_batch_count > 0:
                                compressor.write(b",")  # Add comma between JSON objects
                            compressor.write(json.dumps(game_info).encode('utf-8'))
                            
                            saved_game_count += 1  # Increment saved game counter
                            current_batch_count += 1  # Increment current batch count
                            
                            if saved_game_count % batch_size == 0:
                                print(f"Saved {saved_game_count} games so far in batch {batch_count}...")
                                break
                        
                        compressor.write(b"]")  # End of JSON array

                # If no more games to read, break the outer loop
                if game is None:
                    break

    # Print the final count of parsed and saved games
    print(f"Total number of games parsed: {parsed_game_count}")
    print(f"Total number of games saved: {saved_game_count}")
    print(f"Filtered games saved across {batch_count} batches.")

# Example usage
pgn_file_path = '/sfs/gpfs/tardis/home/zrc3hc/Chess/lichess_db_standard_rated_2024-08.pgn.zst'
output_file_prefix = '/sfs/gpfs/tardis/home/zrc3hc/Chess/datasets/zipped_file/filtered_games'

save_zipped_pgn_to_json_filtered(pgn_file_path, output_file_prefix, event_filter="Rated Blitz game")
