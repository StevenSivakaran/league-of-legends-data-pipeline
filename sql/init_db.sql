-- Créer la base de données pour nos données LoL
CREATE DATABASE lol_data;

-- Se connecter à la base lol_data
\c lol_data

-- Créer le schéma raw pour les données brutes
CREATE SCHEMA IF NOT EXISTS raw;

-- Table des matchs
CREATE TABLE IF NOT EXISTS raw.matches (
    match_id VARCHAR(50) PRIMARY KEY,
    game_creation BIGINT,
    game_duration INTEGER,
    game_mode VARCHAR(50),
    game_type VARCHAR(50),
    game_version VARCHAR(50),
    platform_id VARCHAR(10),
    queue_id INTEGER,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des participants
CREATE TABLE IF NOT EXISTS raw.participants (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) REFERENCES raw.matches(match_id),
    puuid VARCHAR(100),
    summoner_name VARCHAR(100),
    champion_id INTEGER,
    champion_name VARCHAR(50),
    team_id INTEGER,
    role VARCHAR(20),
    lane VARCHAR(20),
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    gold_earned INTEGER,
    total_damage_dealt INTEGER,
    total_damage_taken INTEGER,
    vision_score INTEGER,
    cs INTEGER,
    win BOOLEAN,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des champions
CREATE TABLE IF NOT EXISTS raw.champions (
    champion_id INTEGER PRIMARY KEY,
    champion_key VARCHAR(50),
    champion_name VARCHAR(100),
    title VARCHAR(200),
    tags TEXT[],
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Créer les index pour améliorer les performances
CREATE INDEX IF NOT EXISTS idx_matches_game_creation ON raw.matches(game_creation);
CREATE INDEX IF NOT EXISTS idx_participants_match_id ON raw.participants(match_id);
CREATE INDEX IF NOT EXISTS idx_participants_puuid ON raw.participants(puuid);
CREATE INDEX IF NOT EXISTS idx_participants_champion_id ON raw.participants(champion_id);

-- Message de confirmation
DO $$
BEGIN
    RAISE NOTICE 'Database lol_data initialized successfully!';
END $$;