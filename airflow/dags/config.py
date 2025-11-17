"""
Configuration pour les DAGs League of Legends
"""
import os

# ============================================
# Configuration API Riot
# ============================================

RIOT_API_KEY = os.getenv('RIOT_API_KEY')

# Régions disponibles
REGIONS = {
    'euw1': 'Europe West',
    'na1': 'North America',
    'kr': 'Korea',
    'br1': 'Brazil',
}

# Routings pour l'API Match v5
ROUTINGS = {
    'europe': ['euw1', 'eun1', 'tr1', 'ru'],
    'americas': ['na1', 'br1', 'la1', 'la2'],
    'asia': ['kr', 'jp1'],
}

# Ta région par défaut
DEFAULT_REGION = 'euw1'
DEFAULT_ROUTING = 'europe'

# ============================================
# Configuration Base de Données
# ============================================

POSTGRES_CONN_ID = 'postgres_default'  # ID de connexion dans Airflow

# ============================================
# Configuration de l'extraction
# ============================================

# Combien de matchs récupérer par exécution
MATCHES_PER_RUN = 20

# Queue IDs importants (types de partie)
QUEUE_IDS = {
    420: 'Ranked Solo/Duo',
    440: 'Ranked Flex',
    400: 'Normal Draft',
    430: 'Normal Blind',
    450: 'ARAM',
}

# Par défaut, on récupère uniquement les ranked
DEFAULT_QUEUE_ID = 420

# ============================================
# Configuration Rate Limiting
# ============================================

# Limites de l'API Riot (Development Key)
RATE_LIMIT_PER_SECOND = 20      # 20 requêtes/seconde
RATE_LIMIT_PER_2MIN = 100       # 100 requêtes/2 minutes

# Délai de sécurité entre chaque requête (en secondes)
REQUEST_DELAY = 0.1  # 100ms entre chaque requête = 10 req/sec (sécurité)

# ============================================
# Liste des joueurs à suivre
# ============================================

# Tu peux ajouter plusieurs pseudos à suivre
SUMMONERS_TO_TRACK = [
    ['Steven176', 8555],           # Change par ton pseudo
    ['R U D R A 7', "EUW"],
    ['Vithu', 6835]
]

# ============================================
# Configuration des retries
# ============================================

# Combien de fois réessayer en cas d'erreur
MAX_RETRIES = 3

# Délai entre chaque retry (en secondes)
RETRY_DELAY = 60  # 1 minute