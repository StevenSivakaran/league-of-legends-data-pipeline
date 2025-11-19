/*
    Staging : Nettoyage et standardisation des matchs
    
    Transformations:
    - Conversion du timestamp en datetime
    - Calcul de la durée en minutes
    - Standardisation des noms de colonnes
    - Ajout de colonnes calculées
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'matches') }}
),

cleaned AS (
    SELECT
        -- Identifiants
        match_id,
        platform_id AS region,
        
        -- Dates
        TO_TIMESTAMP(game_creation / 1000) AS game_datetime,
        DATE(TO_TIMESTAMP(game_creation / 1000)) AS game_date,
        game_creation AS game_creation_timestamp,
        
        -- Informations du match
        game_mode,
        game_type,
        queue_id,
        game_version,
        
        -- Durée
        game_duration AS duration_seconds,
        ROUND(game_duration / 60.0, 1) AS duration_minutes,
        
        -- Classification de durée
        CASE
            WHEN game_duration < 900 THEN 'short'      -- < 15min
            WHEN game_duration < 1800 THEN 'medium'    -- 15-30min
            WHEN game_duration < 2700 THEN 'long'      -- 30-45min
            ELSE 'very_long'                           -- > 45min
        END AS game_length_category,
        
        -- Classification du type de partie
        CASE
            WHEN queue_id = 420 THEN 'Ranked Solo/Duo'
            WHEN queue_id = 440 THEN 'Ranked Flex'
            WHEN queue_id = 400 THEN 'Normal Draft'
            WHEN queue_id = 430 THEN 'Normal Blind'
            WHEN queue_id = 450 THEN 'ARAM'
            ELSE 'Other'
        END AS queue_type,
        
        -- Métadonnées
        loaded_at
        
    FROM source
    
    -- Filtrer les matchs invalides
    WHERE game_duration > 0
      AND game_creation > 0
)

SELECT * FROM cleaned