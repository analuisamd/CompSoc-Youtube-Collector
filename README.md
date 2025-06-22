# YouTube Data Collector

Um coletor de vídeos e comentários do YouTube desenvolvido para o projeto da disciplina de Computação Social

1. **Dependências**:
   ```bash
   pip install -r input/requirements.txt
   ```

2. **Input**:
   - `keys.txt`: Chaves API do YouTube (uma por linha)
   - `dates.txt`: Datas de início e fim da coleta (formato DD/MM/AAAA)
   - `channels.txt`: IDs ou nomes dos canais (um por linha)

3. **Uso**:
   ```bash
   python main.py input/keys.txt input/dates.txt input/channels.txt
   ```

## Estrutura de arquivos
```
data/
  Channel1_videos.csv    # Metadados dos vídeos
  Channel1_comments.csv  # Comentários coletados
```
