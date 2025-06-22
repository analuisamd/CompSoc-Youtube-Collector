import os
import csv
import time
import datetime
from typing import List, Dict, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from channel_utils import get_channel_name

class YouTubeDataCollector:
    def __init__(self, api_keys: List[str]):

        self.api_keys = api_keys
        self.current_key_index = 0
        self.youtube = self._build_youtube_service()
        
    def _build_youtube_service(self):
        api_key = self.api_keys[self.current_key_index]
        return build('youtube', 'v3', developerKey=api_key)
    
    def _switch_api_key(self):
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        self.youtube = self._build_youtube_service()
        print(f"Alternando para a chave API {self.current_key_index + 1} de {len(self.api_keys)}")
        time.sleep(1) 
    
    def _handle_api_error(self, error):
        if error.resp.status == 403 and 'quotaExceeded' in str(error):
            print("Cota excedida para a chave API atual. Alternando para próxima chave...")
            self._switch_api_key()
            return True
        elif error.resp.status == 403 and 'disabled' in str(error):
            print("Chave API desabilitada. Alternando para próxima chave...")
            self._switch_api_key()
            return True
        elif error.resp.status == 429:
            print("Muitas requisições. Esperando 60 segundos antes de continuar...")
            time.sleep(60)
            return True
        else:
            print(f"Erro na API: {error}")
            return False
    
    def _parse_date(self, date_str: str) -> datetime.datetime:
        day, month, year = map(int, date_str.split('/'))
        return datetime.datetime(year, month, day, 0, 0, 0)
    
    def _initialize_csv_files(self, channel_id: str, channel_name: str):
        os.makedirs('data', exist_ok=True)
    
        
        if channel_name:
            safe_name = "".join(c for c in channel_name if c.isalnum() or c in (' ', '_')).rstrip()
            base_name = safe_name.replace(' ', '_')[:50]
        else:
            base_name = channel_id
        
        videos_csv = f'data/{base_name}_videos.csv'
        comments_csv = f'data/{base_name}_comments.csv'
        
        videos_header = [
            'video_id', 'title', 'description', 'channel_id', 'published_at', 'category_id',
            'tags', 'view_count', 'like_count', 'comment_count', 'duration', 'definition',
            'caption', 'licensed_content', 'privacy_status', 'license', 'embeddable',
            'public_stats_viewable', 'is_made_for_kids', 'thumbnail_url', 'default_audio_language',
            'default_language', 'actual_start_time', 'scheduled_start_time', 'actual_end_time',
            'scheduled_end_time', 'concurrent_viewers', 'active_live_chat_id', 'recording_date',
            'topicCategories', 'processing_status', 'parts_total', 'parts_processed',
            'time_left_ms', 'processing_failure_reason'
        ]

        comments_header = [
            'video_id', 'comment_id', 'author', 'author_profile_image_url', 'author_channel_url',
            'author_channel_id', 'comment', 'published_at', 'updated_at', 'like_count',
            'viewer_rating', 'can_rate', 'is_reply', 'parent_id', 'channel_id'
        ]
        
        if not os.path.exists(videos_csv):
            with open(videos_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=videos_header)
                writer.writeheader()
                
        if not os.path.exists(comments_csv):
            with open(comments_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=comments_header)
                writer.writeheader()
                
        return videos_csv, comments_csv
    
    def _get_channel_videos(self, channel_id: str, start_date: str, end_date: str):
        videos = []
        next_page_token = None
        
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date)
        
        published_after = start_dt.isoformat() + 'Z'
        published_before = (end_dt + datetime.timedelta(days=1)).isoformat() + 'Z'
        
        while True:
            try:
                request = self.youtube.search().list(
                    part='id',
                    channelId=channel_id,
                    maxResults=50,
                    order='date',
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    type='video',
                    pageToken=next_page_token
                )
                response = request.execute()
                
                video_ids = [item['id']['videoId'] for item in response['items'] if item['id']['kind'] == 'youtube#video']
                videos.extend(video_ids)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                time.sleep(0.1)
                    
            except HttpError as error:
                if not self._handle_api_error(error):
                    break
            except Exception as e:
                print(f"Erro inesperado: {e}")
                break
                
        return videos
    
    def _get_video_details(self, video_id: str) -> Optional[Dict]:
        try:
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics,status,liveStreamingDetails,topicDetails,recordingDetails',
                id=video_id
            )
            response = request.execute()
            
            if not response['items']:
                return None
                
            video = response['items'][0]
            snippet = video.get('snippet', {})
            content_details = video.get('contentDetails', {})
            stats = video.get('statistics', {})
            status = video.get('status', {})
            live_details = video.get('liveStreamingDetails', {})
            topic_details = video.get('topicDetails', {})
            recording_details = video.get('recordingDetails', {})
            
            tags = snippet.get('tags', [])
            tags_str = '|'.join(tags) if tags else ''
            
            topic_categories = topic_details.get('topicCategories', [])
            topic_categories_str = '|'.join(topic_categories) if topic_categories else ''
            
            video_data = {
                'video_id': video_id,
                'title': snippet.get('title', ''),
                'description': snippet.get('description', ''),
                'channel_id': snippet.get('channelId', ''),
                'published_at': snippet.get('publishedAt', ''),
                'category_id': snippet.get('categoryId', ''),
                'tags': tags_str,
                'view_count': stats.get('viewCount', 0),
                'like_count': stats.get('likeCount', 0),
                'comment_count': stats.get('commentCount', 0),
                'duration': content_details.get('duration', ''),
                'definition': content_details.get('definition', ''),
                'caption': content_details.get('caption', ''),
                'licensed_content': content_details.get('licensedContent', False),
                'privacy_status': status.get('privacyStatus', ''),
                'license': status.get('license', ''),
                'embeddable': status.get('embeddable', False),
                'public_stats_viewable': status.get('publicStatsViewable', False),
                'is_made_for_kids': status.get('madeForKids', False),
                'thumbnail_url': snippet.get('thumbnails', {}).get('high', {}).get('url', ''),
                'default_audio_language': snippet.get('defaultAudioLanguage', ''),
                'default_language': snippet.get('defaultLanguage', ''),
                'actual_start_time': live_details.get('actualStartTime', ''),
                'scheduled_start_time': live_details.get('scheduledStartTime', ''),
                'actual_end_time': live_details.get('actualEndTime', ''),
                'scheduled_end_time': live_details.get('scheduledEndTime', ''),
                'concurrent_viewers': live_details.get('concurrentViewers', ''),
                'active_live_chat_id': live_details.get('activeLiveChatId', ''),
                'recording_date': recording_details.get('recordingDate', ''),
                'topicCategories': topic_categories_str,
                'processing_status': status.get('uploadStatus', ''),
                'parts_total': '',
                'parts_processed': '',
                'time_left_ms': '',
                'processing_failure_reason': status.get('failureReason', '')
            }
            
            return video_data
            
        except HttpError as error:
            self._handle_api_error(error)
            return None
        except Exception as e:
            print(f"Erro ao obter detalhes do vídeo {video_id}: {e}")
            return None
    
    def _get_video_comments(self, video_id: str, channel_id: str):
        comments = []
        next_page_token = None
        
        while True:
            try:
                request = self.youtube.commentThreads().list(
                    part='snippet,replies',
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token,
                    textFormat='plainText'
                )
                response = request.execute()
                
                for item in response['items']:
                    comment = self._process_comment(item, video_id, channel_id)
                    comments.append(comment)
                    
                    if 'replies' in item:
                        for reply in item['replies']['comments']:
                            reply_comment = self._process_comment(reply, video_id, channel_id, is_reply=True, parent_id=comment['comment_id'])
                            comments.append(reply_comment)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
                time.sleep(0.1)
                    
            except HttpError as error:
                if error.resp.status == 403 and 'commentsDisabled' in str(error):
                    print(f"Comentários desativados para o vídeo {video_id}")
                    break
                elif not self._handle_api_error(error):
                    break
            except Exception as e:
                print(f"Erro ao obter comentários para o vídeo {video_id}: {e}")
                break
                
        return comments
    
    def _process_comment(self, comment_data, video_id, channel_id, is_reply=False, parent_id=None):
        snippet = comment_data['snippet']
        top_level = snippet.get('topLevelComment', {}).get('snippet', snippet)
        
        author_channel_id = ''
        if 'authorChannelId' in top_level:
            author_channel_id = top_level['authorChannelId'].get('value', '')
        
        return {
            'video_id': video_id,
            'comment_id': comment_data['id'],
            'author': top_level.get('authorDisplayName', ''),
            'author_profile_image_url': top_level.get('authorProfileImageUrl', ''),
            'author_channel_url': top_level.get('authorChannelUrl', ''),
            'author_channel_id': author_channel_id,
            'comment': top_level.get('textDisplay', ''),
            'published_at': top_level.get('publishedAt', ''),
            'updated_at': top_level.get('updatedAt', ''),
            'like_count': top_level.get('likeCount', 0),
            'viewer_rating': top_level.get('viewerRating', ''),
            'can_rate': top_level.get('canRate', False),
            'is_reply': is_reply,
            'parent_id': parent_id if is_reply else '',
            'channel_id': channel_id
        }
    
    def collect_channel_data(self, channel_id: str, start_date: str, end_date: str):
        """Coleta dados de um canal específico dentro de um período."""
        print(f"\nIniciando coleta de dados para o canal {channel_id}")
        print(f"Período: {start_date} a {end_date}")
        
        channel_name = get_channel_name(self.api_keys[0], channel_id)
        display_name = channel_name or channel_id
    
        # Inicializa arquivos CSV
        videos_csv, comments_csv = self._initialize_csv_files(channel_id, channel_name)
        
        print("Obtendo lista de vídeos do canal...")
        video_ids = self._get_channel_videos(channel_id, start_date, end_date)
        print(f"Encontrados {len(video_ids)} vídeos para processar.")
        
        for i, video_id in enumerate(video_ids, 1):
            print(f"\nProcessando vídeo {i} de {len(video_ids)} (ID: {video_id})")
            
            video_data = self._get_video_details(video_id)
            if not video_data:
                print(f"Falha ao obter detalhes do vídeo {video_id}. Pulando...")
                continue
                
            with open(videos_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=video_data.keys())
                writer.writerow(video_data)
            print(f"Dados do vídeo {video_id} salvos com sucesso.")
            
            if int(video_data.get('comment_count', 0)) > 0:
                print(f"Coletando comentários para o vídeo {video_id}...")
                comments = self._get_video_comments(video_id, channel_id)
                
                if comments:
                    with open(comments_csv, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=comments[0].keys())
                        writer.writerows(comments)
                    print(f"Salvos {len(comments)} comentários para o vídeo {video_id}.")
            else:
                print("Vídeo não tem comentários. Pulando coleta de comentários.")
        
        print(f"\nColeta de dados concluída para o canal {channel_id}!")