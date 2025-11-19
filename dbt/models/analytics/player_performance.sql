/*
    Analytics : Performance des joueurs
    
    Analyse détaillée de chaque joueur avec comparaison
*/

{{ config(
    materialized='table'
) }}

WITH player_base AS (
    SELECT * FROM {{ ref('dim_summoners') }}
),

recent_games AS (
    SELECT
        puuid,
        COUNT(*) AS games_last_30_days
    FROM {{ ref('stg_participants') }}
    WHERE loaded_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY puuid
),

player_metrics AS (
    SELECT
        p.puuid,
        p.summoner_name,
        
        -- Volume
        p.total_games,
        COALESCE(r.games_last_30_days, 0) AS recent_games,
        p.unique_champions_played,
        
        -- Performance
        p.win_rate,
        p.total_wins,
        
        -- Stats moyennes
        p.avg_kills,
        p.avg_deaths,
        p.avg_assists,
        p.avg_kda,
        
        -- Économie
        p.avg_gold,
        p.avg_gold_per_min,
        
        -- Combat
        p.avg_damage,
        p.avg_damage_taken,
        
        -- Farm
        p.avg_cs,
        p.avg_cs_per_min,
        
        -- Vision
        p.avg_vision_score,
        
        -- Préférences
        p.preferred_role,
        p.most_played_champion,
        
        -- Ranking
        NTILE(10) OVER (ORDER BY p.avg_kda) AS kda_decile,
        NTILE(10) OVER (ORDER BY p.win_rate) AS winrate_decile,
        
        -- Classification
        CASE
            WHEN p.kda_percentile >= 0.9 THEN 'Elite'
            WHEN p.kda_percentile >= 0.7 THEN 'Advanced'
            WHEN p.kda_percentile >= 0.5 THEN 'Intermediate'
            WHEN p.kda_percentile >= 0.3 THEN 'Beginner'
            ELSE 'Learning'
        END AS skill_tier,
        
        -- Activité
        p.first_game,
        p.last_game,
        CURRENT_DATE - DATE(p.last_game) AS days_since_last_game,
        
        -- Status
        CASE
            WHEN CURRENT_DATE - DATE(p.last_game) <= 7 THEN 'Active'
            WHEN CURRENT_DATE - DATE(p.last_game) <= 30 THEN 'Occasional'
            ELSE 'Inactive'
        END AS activity_status,
        
        CURRENT_TIMESTAMP AS calculated_at
        
    FROM player_base p
    LEFT JOIN recent_games r ON p.puuid = r.puuid
)

SELECT * FROM player_metrics
ORDER BY total_games DESC