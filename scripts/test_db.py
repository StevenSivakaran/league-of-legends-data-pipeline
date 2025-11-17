"""
Script de test de connexion à la base de données
"""
import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def test_connection():
    """Test de connexion à PostgreSQL"""
    try:
        # Connexion
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        
        cursor = conn.cursor()
        
        # Test 1 : Vérifier que les tables existent
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'raw'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print("✅ Tables trouvées :")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Test 2 : Compter les lignes
        cursor.execute("SELECT COUNT(*) FROM raw.matches")
        matches_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM raw.participants")
        participants_count = cursor.fetchone()[0]
        
        print(f"\n📊 Données actuelles :")
        print(f"   - Matches : {matches_count}")
        print(f"   - Participants : {participants_count}")
        
        # Test 3 : Insérer une donnée de test
        cursor.execute("""
            INSERT INTO raw.matches (
                match_id, game_creation, game_duration, 
                game_mode, queue_id
            ) VALUES (
                'TEST_123', 1699876543000, 1845, 
                'CLASSIC', 420
            )
            ON CONFLICT (match_id) DO NOTHING
            RETURNING match_id;
        """)
        
        if cursor.fetchone():
            print("\n✅ Test d'insertion réussi !")
            
            # Nettoyage
            cursor.execute("DELETE FROM raw.matches WHERE match_id = 'TEST_123'")
            conn.commit()
            print("✅ Test de suppression réussi !")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Tous les tests sont passés !")
        
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    test_connection()