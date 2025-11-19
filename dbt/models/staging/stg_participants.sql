/*
    Staging : Nettoyage et standardisation des participants
    
    Transformations:
    - Calcul du KDA
    - Calcul des stats par minute
    - Classification des rôles
    - Nettoyage des données aberrantes
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'participants') }}
),

matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
),

cleaned AS (
    SELECT
        -- Identifiants
        p.id AS participant_id,
        p.match_id,
        p.puuid,
        COALESCE(p.summoner_name, 'Unknown') AS summoner_name,
        
        -- Champion
        p.champion_id,
        p.champion_name,
        
        -- Équipe et position
        p.team_id,
        CASE
            WHEN p.team_id = 100 THEN 'Blue'
            WHEN p.team_id = 200 THEN 'Red'
            ELSE 'Unknown'
        END AS team_side,
        
        COALESCE(p.role, 'UNKNOWN') AS role,
        COALESCE(p.lane, 'UNKNOWN') AS lane,
        
        -- Standardisation des rôles
        CASE
            WHEN p.role = 'TOP' THEN 'Top'
            WHEN p.role IN ('JUNGLE', 'NONE') AND p.lane = 'JUNGLE' THEN 'Jungle'
            WHEN p.role IN ('MIDDLE', 'MID') THEN 'Mid'
            WHEN p.role IN ('BOTTOM', 'CARRY') THEN 'ADC'
            WHEN p.role IN ('UTILITY', 'SUPPORT') THEN 'Support'
            ELSE 'Unknown'
        END AS standardized_role,
        
        -- Statistiques de combat
        p.kills,
        p.deaths,
        p.assists,
        
        -- Calcul du KDA
        CASE
            WHEN p.deaths = 0 THEN p.kills + p.assists  -- KDA parfait
            ELSE ROUND((p.kills + p.assists)::NUMERIC / p.deaths, 2)
        END AS kda,
        
        -- Classification KDA
        CASE
            WHEN p.deaths = 0 AND (p.kills + p.assists) > 5 THEN 'Perfect'
            WHEN (p.kills + p.assists)::NUMERIC / NULLIF(p.deaths, 0) >= 3 THEN 'Excellent'
            WHEN (p.kills + p.assists)::NUMERIC / NULLIF(p.deaths, 0) >= 2 THEN 'Good'
            WHEN (p.kills + p.assists)::NUMERIC / NULLIF(p.deaths, 0) >= 1 THEN 'Average'
            ELSE 'Poor'
        END AS kda_rating,
        
        -- Statistiques économiques
        p.gold_earned,
        
        -- Statistiques de dégâts
        p.total_damage_dealt AS damage_dealt,
        p.total_damage_taken AS damage_taken,
        
        -- Farm
        p.cs AS total_cs,
        
        -- CS par minute (jointure avec matches pour avoir la durée)
        ROUND(
            p.cs::NUMERIC / NULLIF(m.duration_minutes, 0),
            1
        ) AS cs_per_min,
        
        -- Classification du farm
        CASE
            WHEN ROUND(p.cs::NUMERIC / NULLIF(m.duration_minutes, 0), 1) >= 8 THEN 'Excellent'
            WHEN ROUND(p.cs::NUMERIC / NULLIF(m.duration_minutes, 0), 1) >= 6 THEN 'Good'
            WHEN ROUND(p.cs::NUMERIC / NULLIF(m.duration_minutes, 0), 1) >= 4 THEN 'Average'
            ELSE 'Poor'
        END AS cs_rating,
        
        -- Vision
        p.vision_score,
        ROUND(
            p.vision_score::NUMERIC / NULLIF(m.duration_minutes, 0),
            1
        ) AS vision_per_min,
        
        -- Résultat
        p.win,
        
        -- Gold efficiency (gold per minute)
        ROUND(
            p.gold_earned::NUMERIC / NULLIF(m.duration_minutes, 0),
            0
        ) AS gold_per_min,
        
        -- Damage efficiency
        ROUND(
            p.total_damage_dealt::NUMERIC / NULLIF(p.gold_earned, 0),
            2
        ) AS damage_per_gold,
        
        -- Métadonnées
        p.loaded_at
        
    FROM source p
    INNER JOIN matches m ON p.match_id = m.match_id
    
    -- Filtrer les données aberrantes
    WHERE p.kills >= 0
      AND p.deaths >= 0
      AND p.assists >= 0
      AND p.champion_name IS NOT NULL
)

SELECT * FROM cleaned