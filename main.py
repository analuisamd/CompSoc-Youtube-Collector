import sys
from youtube_collector import YouTubeDataCollector
from channel_utils import read_channels_from_file, read_dates_from_file, get_channel_id

def main():
    if len(sys.argv) != 4:
        print("Uso: python main.py <api_keys_file> <dates_file> <channels_file>")
        return
    
    api_keys_file = sys.argv[1]
    dates_file = sys.argv[2]
    channels_file = sys.argv[3]
    
    try:
        with open(api_keys_file, 'r') as f:
            api_keys = [line.strip() for line in f.readlines() if line.strip()]
        
        if not api_keys:
            print("Nenhuma chave API válida encontrada.")
            return
        
        start_date, end_date = read_dates_from_file(dates_file)
        channels = read_channels_from_file(channels_file)
        collector = YouTubeDataCollector(api_keys)
        

        for channel in channels:
            if not channel.startswith('UC'):
                print(f"\nObtendo ID para o canal {channel}...")
                channel_id = get_channel_id(api_keys[0], channel)
                if not channel_id:
                    print(f"Não foi possível obter ID para o canal {channel}. Pulando...")
                    continue
            else:
                channel_id = channel
            collector.collect_channel_data(channel_id, start_date, end_date)
            
    except Exception as e:
        print(f"Erro durante a execução: {e}")

if __name__ == "__main__":
    main()