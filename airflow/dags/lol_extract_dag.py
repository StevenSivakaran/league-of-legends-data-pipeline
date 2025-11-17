"""
DAG Airflow pour extraire les données League of Legends
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import time
import json
import logging

# Import de notre config
from config import (
    RIOT_API_KEY,
    DEFAULT_REGION,
    DEFAULT_ROUTING,
    MATCHES_PER_RUN,
    REQUEST_DELAY,
    POSTGRES_CONN_ID,
    SUMMONERS_TO_TRACK,
    DEFAULT_QUEUE_ID
)

# Configuration du logger
logger = logging.getLogger(__name__)

# ============================================
# Configuration du DAG
# ============================================

default_args = {
    'owner': 'steven',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# Fonctions utilitaires pour l'API Riot
# ============================================

def make_riot_api_request(url: str, params: dict = None) -> dict:
    """
    Fait une requête à l'API Riot avec gestion d'erreurs
    
    Args:
        url: URL de l'endpoint
        params: Paramètres de la requête
    
    Returns:
        dict: Réponse JSON de l'API
    
    Raises:
        Exception: Si la requête échoue après plusieurs tentatives
    """
    headers = {'X-Riot-Token': RIOT_API_KEY}
    
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            # Gérer les différents codes d'erreur
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limit atteint
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit hit, waiting {retry_after} seconds")
                time.sleep(retry_after)
            elif response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                return None
            else:
                logger.error(f"API Error {response.status_code}: {response.text}")
                
            # Attendre avant de réessayer
            time.sleep(2 ** attempt)  # Exponential backoff
            
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {attempt + 1}/3)")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise
    
    raise Exception(f"Failed to fetch {url} after 3 attempts")

def get_summoner_puuid(summoner_name_lol: str, tagline: str, routing: str = DEFAULT_ROUTING) -> str:
    """
    Récupère le PUUID d'un invocateur par son nom
    
    Args:
        summoner_name: Nom de l'invocateur
        region: Région du serveur
    
    Returns:
        str: PUUID de l'invocateur
    """
    url = f"https://{routing}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{summoner_name_lol}/{tagline}"
    data = make_riot_api_request(url)
    
    if data:
        logger.info(f"✅ Found summoner {summoner_name_lol}: {data['puuid']}")
        return data['puuid']
    else:
        raise Exception(f"Summoner {summoner_name_lol} not found")

def get_match_ids(puuid: str, routing: str = DEFAULT_ROUTING, count: int = 20, queue_id: int = None) -> list:
    """
    Récupère les IDs des matchs d'un joueur
    
    Args:
        puuid: PUUID du joueur
        count: Nombre de matchs à récupérer
        queue_id: Type de partie (420 = Ranked Solo)
    
    Returns:
        list: Liste des match IDs
    """
    url = f"https://{routing}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"

    params = {
        'start': 0,
        'count': count
    }
    
    if queue_id:
        params['queue'] = queue_id
    
    match_ids = make_riot_api_request(url, params)
    logger.info(f"✅ Found {len(match_ids)} matches")
    
    return match_ids

def get_match_details(match_id: str, routing: str = DEFAULT_ROUTING) -> dict:
    """
    Récupère les détails d'un match
    
    Args:
        match_id: ID du match
    
    Returns:
        dict: Détails du match
    """
    url = f"https://{routing}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return make_riot_api_request(url)

# ============================================
# Fonctions de traitement des données
# ============================================

def check_match_exists(cursor, match_id: str) -> bool:
    """Vérifie si un match existe déjà dans la DB"""
    cursor.execute("SELECT 1 FROM raw.matches WHERE match_id = %s", (match_id,))
    return cursor.fetchone() is not None

def insert_match(cursor, match_id: str, match_data: dict):
    """Insère un match dans la base de données"""
    info = match_data['info']
    
    cursor.execute("""
        INSERT INTO raw.matches (
            match_id, game_creation, game_duration, game_mode,
            game_type, game_version, platform_id, queue_id, raw_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO NOTHING
    """, (
        match_id,
        info['gameCreation'],
        info['gameDuration'],
        info['gameMode'],
        info['gameType'],
        info['gameVersion'],
        info['platformId'],
        info['queueId'],
        json.dumps(match_data)
    ))

def insert_participants(cursor, match_id: str, participants: list):
    """Insère les participants d'un match"""
    for participant in participants:
        cursor.execute("""
            INSERT INTO raw.participants (
                match_id, puuid, summoner_name, champion_id, champion_name,
                team_id, role, lane, kills, deaths, assists, gold_earned,
                total_damage_dealt, total_damage_taken, vision_score, cs, win, raw_data
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            match_id,
            participant['puuid'],
            participant['riotIdGameName'] if 'riotIdGameName' in participant else participant.get('summonerName', 'Unknown'),
            participant['championId'],
            participant['championName'],
            participant['teamId'],
            participant.get('teamPosition', ''),
            participant.get('lane', ''),
            participant['kills'],
            participant['deaths'],
            participant['assists'],
            participant['goldEarned'],
            participant['totalDamageDealtToChampions'],
            participant['totalDamageTaken'],
            participant['visionScore'],
            participant['totalMinionsKilled'] + participant['neutralMinionsKilled'],
            participant['win'],
            json.dumps(participant)
        ))

# ============================================
# Tasks Airflow
# ============================================

def extract_and_load_matches(**context):
    """
    Task principale : Extraction et chargement des matchs
    """
    logger.info("🚀 Starting extraction process")
    
    # Statistiques
    stats = {
        'summoners_processed': 0,
        'matches_found': 0,
        'matches_inserted': 0,
        'matches_skipped': 0,
        'participants_inserted': 0,
        'errors': 0
    }
    
    # Connexion à PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Pour chaque joueur à tracker
        for summoner_name in SUMMONERS_TO_TRACK:
            try:
                summoner_name_lol = summoner_name[0]
                tagline = summoner_name[1]
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing summoner: {summoner_name_lol}")
                logger.info(f"{'='*50}")
                
                # 1. Récupérer le PUUID
                puuid = get_summoner_puuid(summoner_name_lol, tagline)
                time.sleep(REQUEST_DELAY)
                
                # 2. Récupérer les match IDs
                match_ids = get_match_ids(puuid, count=MATCHES_PER_RUN, queue_id=DEFAULT_QUEUE_ID)
                stats['matches_found'] += len(match_ids)
                time.sleep(REQUEST_DELAY)
                
                # 3. Pour chaque match
                for i, match_id in enumerate(match_ids, 1):
                    try:
                        logger.info(f"\n[{i}/{len(match_ids)}] Processing match: {match_id}")
                        
                        # Vérifier si le match existe déjà
                        if check_match_exists(cursor, match_id):
                            logger.info(f"  ⏭️  Match already exists, skipping")
                            stats['matches_skipped'] += 1
                            continue
                        
                        # Récupérer les détails du match
                        match_data = get_match_details(match_id)
                        time.sleep(REQUEST_DELAY)
                        
                        if not match_data:
                            logger.warning(f"  ⚠️  No data for match {match_id}")
                            continue
                        
                        # Insérer le match
                        insert_match(cursor, match_id, match_data)
                        stats['matches_inserted'] += 1
                        logger.info(f"  ✅ Match inserted")
                        
                        # Insérer les participants
                        participants = match_data['info']['participants']
                        insert_participants(cursor, match_id, participants)
                        stats['participants_inserted'] += len(participants)
                        logger.info(f"  ✅ {len(participants)} participants inserted")
                        
                        # Commit après chaque match
                        conn.commit()
                        
                    except Exception as e:
                        logger.error(f"  ❌ Error processing match {match_id}: {str(e)}")
                        stats['errors'] += 1
                        conn.rollback()
                        continue
                
                stats['summoners_processed'] += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing summoner {summoner_name[0]}: {str(e)}")
                stats['errors'] += 1
                continue
        
        # Logs finaux
        logger.info("\n" + "="*50)
        logger.info("📊 EXTRACTION SUMMARY")
        logger.info("="*50)
        logger.info(f"Summoners processed: {stats['summoners_processed']}")
        logger.info(f"Matches found: {stats['matches_found']}")
        logger.info(f"Matches inserted: {stats['matches_inserted']}")
        logger.info(f"Matches skipped: {stats['matches_skipped']}")
        logger.info(f"Participants inserted: {stats['participants_inserted']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("="*50)
        
    finally:
        cursor.close()
        conn.close()
    
    # Retourner les stats pour XCom (communication entre tasks)
    return stats

# ============================================
# Définition du DAG
# ============================================

with DAG(
    dag_id='lol_extract_matches',
    default_args=default_args,
    description='Extract League of Legends match data from Riot API',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,  # Ne pas rattraper les exécutions passées
    tags=['lol', 'extraction', 'riot-api'],
    max_active_runs=1,  # Une seule exécution à la fois
) as dag:
    
    # Task d'extraction
    extract_task = PythonOperator(
        task_id='extract_and_load_matches',
        python_callable=extract_and_load_matches,
        provide_context=True,
    )