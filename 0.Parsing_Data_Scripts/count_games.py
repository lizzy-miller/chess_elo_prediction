import zstandard as zstd
import io
import chess.pgn

def count_games_in_zipped_pgn(pgn_file):
    parsed_game_count = 0  # Counter for how many games have been parsed
    
    # Open the compressed input PGN file and set up Zstandard decompression
    with open(pgn_file, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        
        # Decompress the stream in memory
        with dctx.stream_reader(compressed_file) as reader:
            decompressed_stream = io.TextIOWrapper(reader, encoding='utf-8')
            
            # Read the PGN games from the decompressed stream and count them
            while True:
                game = chess.pgn.read_game(decompressed_stream)
                if game is None:
                    break
                
                # Increment parsed game counter
                parsed_game_count += 1

    # Print the total number of parsed games
    print(f"Total number of games in {pgn_file}: {parsed_game_count}")
    return parsed_game_count

# Example usage
pgn_file_path = '/sfs/gpfs/tardis/home/zrc3hc/Chess/datasets/zipped_file/filtered_games.zst'

count_games_in_zipped_pgn(pgn_file_path)
