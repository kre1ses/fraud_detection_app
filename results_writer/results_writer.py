import os
import json
import logging
import psycopg2
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/results_writer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ResultsWriter:
    def __init__(self):
        self.kafka_config = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            'group.id': 'results-writer',
            'auto.offset.reset': 'earliest'
        }
        
        self.db_config = {
            'host': os.getenv("POSTGRES_HOST", "postgres"),
            'database': os.getenv("POSTGRES_DB", "fraud_detection"),
            'user': os.getenv("POSTGRES_USER", "postgres"),
            'password': os.getenv("POSTGRES_PASSWORD", "postgres"),
            'port': os.getenv("POSTGRES_PORT", "5432")
        }
        
        self.topic = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
        
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe([self.topic])
        
        self.conn = self.connect_to_db()
        
    def connect_to_db(self):
        """Подключение к PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_config)
            logger.info("Successfully connected to PostgreSQL database")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def save_to_db(self, transaction_id, score, fraud_flag):
        """Сохранение результата в БД"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO scoring_results (transaction_id, score, fraud_flag)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    fraud_flag = EXCLUDED.fraud_flag,
                    created_at = CURRENT_TIMESTAMP
                """, (transaction_id, score, fraud_flag))
                self.conn.commit()
                logger.info(f"Saved result for transaction {transaction_id}: score={score}, fraud={fraud_flag}")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            self.conn.rollback()
    
    def process_messages(self):
        """Обработка сообщений из Kafka"""
        logger.info("Starting results writer service...")
        
        while True:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                if isinstance(data, list) and len(data) > 0:
                    result = data[0]  
                    
                    transaction_id = result.get('transaction_id')
                    score = float(result.get('score', 0))
                    fraud_flag = bool(result.get('fraud_flag', False))
                    
                    if transaction_id:
                        self.save_to_db(transaction_id, score, fraud_flag)
                    else:
                        logger.warning("Missing transaction_id in message")
                else:
                    logger.warning("Unexpected message format")
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    def close(self):
        """Закрытие соединений"""
        self.consumer.close()
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    writer = ResultsWriter()
    try:
        writer.process_messages()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    finally:
        writer.close()