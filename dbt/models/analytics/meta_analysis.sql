/*
    Analytics : Analyse de la méta
    
    Tendances globales, champions OP, champions underperforming
*/

{{ config(
    materialized='table'
) }}

WITH champion_stats AS (
    SELECT * FROM {{ ref('champion_stats') }}
),

role_distribution AS (
    SELECT
        most_played_role AS role,
        COUNT(*) AS champion_count,
        AVG(win_rate) AS avg_role_winrate
    FROM champion_stats
    GROUP BY most_played_role
),

meta_insights AS (
    SELECT
        -- Top performers (OP champions)
        (
            SELECT JSON_AGG(
                JSON_BUILD_OBJECT(
                    'champion', champion_name,
                    'winrate', win_rate,
                    'games', total_games
                )
            )
            FROM (
                SELECT champion_name, win_rate, total_games
                FROM champion_stats
                WHERE total_games >= 20  -- Échantillon significatif
                ORDER BY win_rate DESC
                LIMIT 10
            ) top_10
        ) AS top_champions,
        
        -- Worst performers
        (
            SELECT JSON_AGG(
                JSON_BUILD_OBJECT(
                    'champion', champion_name,
                    'winrate', win_rate,
                    'games', total_games
                )
            )
            FROM (
                SELECT champion_name, win_rate, total_games
                FROM champion_stats
                WHERE total_games >= 20
                ORDER BY win_rate ASC
                LIMIT 10
            ) bottom_10
        ) AS worst_champions,
        
        -- Most popular
        (
            SELECT JSON_AGG(
                JSON_BUILD_OBJECT(
                    'champion', champion_name,
                    'pick_rate', pick_rate,
                    'winrate', win_rate
                )
            )
            FROM (
                SELECT champion_name, pick_rate, win_rate
                FROM champion_stats
                ORDER BY pick_rate DESC
                LIMIT 10
            ) most_popular
        ) AS most_popular_champions,
        
        -- Stats globales
        (SELECT AVG(win_rate) FROM champion_stats) AS global_avg_winrate,
        (SELECT AVG(avg_kda) FROM champion_stats) AS global_avg_kda,
        (SELECT COUNT(*) FROM champion_stats) AS total_champions_played,
        
        CURRENT_TIMESTAMP AS calculated_at
)

SELECT * FROM meta_insights