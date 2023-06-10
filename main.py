from flask import Flask, jsonify
import os
import feedparser
import time
from bs4 import BeautifulSoup
import tweepy
from apscheduler.schedulers.background import BackgroundScheduler
import tweepy.errors
import psycopg2

# Crie a aplicação Flask
app = Flask(__name__)

# URL do feed
FEED_URL = 'https://agrozil.com.br/feed/'

# Chaves API movidas para variáveis de ambiente
api_key = os.environ.get("API_KEY")
api_secret = os.environ.get("API_SECRET")
bearer_token = os.environ.get("BEARER_TOKEN")
access_token = os.environ.get("ACCESS_TOKEN")
access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET")

client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# Conecte-se ao banco de dados PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
cursor = conn.cursor()

# Crie a tabela para armazenar os GUIDs se ainda não existir
cursor.execute("CREATE TABLE IF NOT EXISTS guid_list (guid TEXT UNIQUE)")
conn.commit()

def get_feed_entries(feed_url):
    try:
        feed = feedparser.parse(feed_url)
        return feed.entries
    except Exception as e:
        print(f"Erro ao buscar o feed: {str(e)}")
        return []

def get_second_post_info(entries):
    try:
        second_post = entries[1]
        guid = second_post.guid
        link = second_post.link
        description = second_post.description
        soup = BeautifulSoup(description, 'html.parser')
        excerpt = soup.find('p').text
        return guid, link, excerpt
    except tweepy.TweepyException as e:
        if '429' in str(e):
            print("Muitas solicitações. Espere um momento antes de tentar novamente.")
        else:
            print(f"Erro ao enviar tweet: {str(e)}")

def check_and_trim_list(limit):
    cursor.execute("DELETE FROM guid_list WHERE ctid IN (SELECT ctid FROM guid_list ORDER BY guid DESC OFFSET %s)", (limit,))
    conn.commit()

def check_feed():
    entries = get_feed_entries(FEED_URL)
    if entries:
        new_guid, new_link, new_excerpt = get_second_post_info(entries)
        cursor.execute("SELECT guid FROM guid_list WHERE guid = %s", (new_guid,))
        already_exists = cursor.fetchone() is not None
        if new_guid and not already_exists:
            cursor.execute("INSERT INTO guid_list (guid) VALUES (%s) ON CONFLICT (guid) DO NOTHING", (new_guid,))
            check_and_trim_list(100)
            conn.commit()
            try:
                client.create_tweet(text = f'{new_excerpt} {new_link}')
                time.sleep(5) # espera 5 segundos antes de enviar outro tweet
            except tweepy.TweepyException as e:
                if '429' in str(e):
                    print("Muitas solicitações. Espere um momento antes de tentar novamente.")
                else:
                    print(f"Erro ao enviar tweet: {str(e)}")

@app.route("/")
def hello():
    return "O aplicativo está funcionando!"

# Na primeira execução, salve todos os GUIDs e poste o segundo item mais recente
entries = get_feed_entries(FEED_URL)
if entries:
    for entry in entries:
        cursor.execute("INSERT INTO guid_list (guid) VALUES (%s) ON CONFLICT (guid) DO NOTHING", (entry.guid,))
    conn.commit()
    new_guid, new_link, new_excerpt = get_second_post_info(entries)
    client.create_tweet(text = f'{new_excerpt} {new_link}')

# Configurar o agendador para verificar o feed a cada 10 minutos
sched = BackgroundScheduler(daemon=True)
sched.add_job(check_feed,'interval',minutes=10)
sched.start()

if __name__ == "__main__":
    app.run()
