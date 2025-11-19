/*
    Analytics : Statistiques détaillées par champion
    
    Vue complète des performances de chaque champion avec ranking
*/

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['champion_name']},
        {'columns': ['win_rate']},
        {'columns': ['pick_rate']}
    ]
) }}

WITH champion_base AS (
    SELECT * FROM {{ ref('dim_champions') }}
),

all_matches AS (
    SELECT COUNT(DISTINCT match_id) AS total_matches
    FROM {{ ref('stg_participants') }}
),

champion_metrics AS (
    SELECT
        c.champion_name,
        c.champion_id,
        
        -- Volume et popularité
        c.total_games,
        ROUND(
            (c.total_games::NUMERIC / m.total_matches) * 100,
            2
        ) AS pick_rate,
        
        -- Performance
        c.win_rate,
        c.total_wins,
        
        -- Ranking
        RANK() OVER (ORDER BY c.win_rate DESC) AS winrate_rank,
        RANK() OVER (ORDER BY c.total_games DESC) AS popularity_rank,
        
        -- KDA
        c.avg_kills,
        c.avg_deaths,
        c.avg_assists,
        c.avg_kda,
        
        -- Combat
        c.avg_damage_dealt,
        c.avg_damage_taken,
        c.avg_gold_earned,
        
        -- Farm
        c.avg_cs,
        c.avg_cs_per_min,
        
        -- Vision
        c.avg_vision_score,
        
        -- Rôle principal
        c.most_played_role,
        
        -- Classification tier
        CASE
            WHEN c.win_rate >= 53 AND c.total_games >= 50 THEN 'S'
            WHEN c.win_rate >= 51 AND c.total_games >= 30 THEN 'A'
            WHEN c.win_rate >= 49 AND c.total_games >= 20 THEN 'B'
            WHEN c.win_rate >= 47 AND c.total_games >= 10 THEN 'C'
            ELSE 'D'
        END AS tier,
        
        -- Métadonnées
        c.first_seen,
        c.last_seen,
        CURRENT_TIMESTAMP AS calculated_at
        
    FROM champion_base c
    CROSS JOIN all_matches m
)

SELECT * FROM champion_metrics
ORDER BY total_games DESC