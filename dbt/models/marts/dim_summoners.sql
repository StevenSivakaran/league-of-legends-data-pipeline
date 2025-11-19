/*
    Dimension : Summoners (Joueurs)
    
    Profil complet de chaque joueur avec ses stats globales
*/

WITH participant_stats AS (
    SELECT * FROM {{ ref('stg_participants') }}
),

summoner_aggregates AS (
    SELECT
        -- Identifiants
        puuid,
        MAX(summoner_name) AS summoner_name,  -- Prend le plus récent
        
        -- Volume
        COUNT(*) AS total_games,
        COUNT(DISTINCT match_id) AS total_matches,
        COUNT(DISTINCT champion_name) AS unique_champions_played,
        
        -- Performance globale
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
        
        -- Performance économique
        ROUND(AVG(gold_earned), 0) AS avg_gold,
        ROUND(AVG(gold_per_min), 0) AS avg_gold_per_min,
        
        -- Combat
        ROUND(AVG(damage_dealt), 0) AS avg_damage,
        ROUND(AVG(damage_taken), 0) AS avg_damage_taken,
        
        -- Farm
        ROUND(AVG(total_cs), 1) AS avg_cs,
        ROUND(AVG(cs_per_min), 1) AS avg_cs_per_min,
        
        -- Vision
        ROUND(AVG(vision_score), 1) AS avg_vision_score,
        
        -- Rôles préférés
        MODE() WITHIN GROUP (ORDER BY standardized_role) AS preferred_role,
        
        -- Champion le plus joué
        MODE() WITHIN GROUP (ORDER BY champion_name) AS most_played_champion,
        
        -- Activité
        MIN(loaded_at) AS first_game,
        MAX(loaded_at) AS last_game,
        
        -- Classement des performances
        PERCENT_RANK() OVER (ORDER BY AVG(kda)) AS kda_percentile,
        PERCENT_RANK() OVER (ORDER BY AVG(CASE WHEN win THEN 1.0 ELSE 0.0 END)) AS winrate_percentile
        
    FROM participant_stats
    GROUP BY puuid
)

SELECT * FROM summoner_aggregates