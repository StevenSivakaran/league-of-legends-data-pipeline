/*
    Dimension : Champions
    
    Table qui agrège toutes les stats par champion
    Utilisée pour l'analyse de la méta et les comparaisons
*/

WITH participant_stats AS (
    SELECT * FROM {{ ref('stg_participants') }}
),

champion_aggregates AS (
    SELECT
        champion_name,
        champion_id,
        
        -- Volume
        COUNT(*) AS total_games,
        COUNT(DISTINCT match_id) AS total_matches,
        
        -- Performance
        SUM(CASE WHEN win THEN 1 ELSE 0 END) AS total_wins,
        ROUND(
            AVG(CASE WHEN win THEN 1.0 ELSE 0.0 END) * 100,
            2
        ) AS win_rate,
        
        -- Statistiques moyennes
        ROUND(AVG(kills), 2) AS avg_kills,
        ROUND(AVG(deaths), 2) AS avg_deaths,
        ROUND(AVG(assists), 2) AS avg_assists,
        ROUND(AVG(kda), 2) AS avg_kda,
        
        -- Farm
        ROUND(AVG(total_cs), 1) AS avg_cs,
        ROUND(AVG(cs_per_min), 1) AS avg_cs_per_min,
        
        -- Combat
        ROUND(AVG(damage_dealt), 0) AS avg_damage_dealt,
        ROUND(AVG(damage_taken), 0) AS avg_damage_taken,
        ROUND(AVG(gold_earned), 0) AS avg_gold_earned,
        
        -- Vision
        ROUND(AVG(vision_score), 1) AS avg_vision_score,
        
        -- Popularité par rôle
        MODE() WITHIN GROUP (ORDER BY standardized_role) AS most_played_role,
        
        -- Dates
        MIN(loaded_at) AS first_seen,
        MAX(loaded_at) AS last_seen,
        
        -- Métadonnées
        COUNT(DISTINCT puuid) AS unique_players
        
    FROM participant_stats
    GROUP BY champion_name, champion_id
)

SELECT * FROM champion_aggregates