/*
    Table de faits : Matchs
    
    Regroupe toutes les informations d'un match avec métriques calculées
    Format wide pour faciliter les analyses
*/

WITH matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
),

participants AS (
    SELECT * FROM {{ ref('stg_participants') }}
),

match_summary AS (
    SELECT
        m.match_id,
        m.game_datetime,
        m.game_date,
        m.duration_minutes,
        m.game_length_category,
        m.queue_type,
        m.game_version,
        m.region,
        
        -- Statistiques globales du match
        AVG(p.kills) AS avg_kills,
        AVG(p.deaths) AS avg_deaths,
        AVG(p.assists) AS avg_assists,
        SUM(p.total_cs) AS total_cs,
        AVG(p.vision_score) AS avg_vision_score,
        
        -- Comptage des joueurs
        COUNT(*) AS total_players,
        
        -- Team metrics
        SUM(CASE WHEN p.team_side = 'Blue' AND p.win THEN 1 ELSE 0 END) AS blue_wins,
        SUM(CASE WHEN p.team_side = 'Red' AND p.win THEN 1 ELSE 0 END) AS red_wins,
        
        -- Champion diversity
        COUNT(DISTINCT p.champion_name) AS unique_champions,
        
        -- Métadonnées
        m.loaded_at
        
    FROM matches m
    LEFT JOIN participants p ON m.match_id = p.match_id
    GROUP BY 
        m.match_id,
        m.game_datetime,
        m.game_date,
        m.duration_minutes,
        m.game_length_category,
        m.queue_type,
        m.game_version,
        m.region,
        m.loaded_at
)

SELECT * FROM match_summary