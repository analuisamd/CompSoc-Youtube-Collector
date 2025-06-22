from googleapiclient.discovery import build
from typing import Optional
from typing import List

def get_channel_id(api_key: str, channel_name: str) -> Optional[str]:
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    try:
        request = youtube.search().list(
            q=channel_name,
            part='snippet',
            type='channel',
            maxResults=1
        )
        response = request.execute()
        
        if response['items']:
            return response['items'][0]['id']['channelId']
        return None
    except Exception as e:
        print(f"Erro ao obter ID do canal {channel_name}: {e}")
        return None

def read_channels_from_file(file_path: str) -> List[str]:
    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    return lines

def read_dates_from_file(file_path: str) -> tuple:
    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    
    if len(lines) >= 2:
        return lines[0], lines[1]
    raise ValueError("Deve conter data inicial e data final")


def get_channel_name(api_key: str, channel_id: str) -> Optional[str]:
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    try:
        request = youtube.channels().list(
            part='snippet',
            id=channel_id
        )
        response = request.execute()
        
        if response['items']:
            return response['items'][0]['snippet']['title']
        return None
    except Exception as e:
        print(f"Erro ao obter nome do canal {channel_id}: {e}")
        return None