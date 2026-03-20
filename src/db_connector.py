import oracledb
import psycopg2
import pymysql
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Teleport.DBConnector')

class DBConnector:
    def __init__(self, config):
        self.config = config

    @contextmanager
    def get_oracle_connection(self):
        """Oracle 데이터베이스 커넥션 생성"""
        conn = None
        try:
            # Thin mode by default in oracledb
            dsn = f"{self.config['host']}:{self.config['port']}/{self.config['service_name']}"
            logger.info(f"Connecting to Oracle DB: {dsn}")
            conn = oracledb.connect(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn
            )
            yield conn
        except oracledb.Error as e:
            logger.error(f"Oracle connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Oracle connection closed.")

    @contextmanager
    def get_postgres_connection(self):
        """PostgreSQL 데이터베이스 커넥션 생성"""
        conn = None
        try:
            logger.info(f"Connecting to PostgreSQL DB: {self.config['host']}:{self.config['port']}")
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            yield conn
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("PostgreSQL connection closed.")

    def test_connection(self, db_type="oracle"):
        """커넥션 테스트 및 권한 확인 (Pre-flight Check)"""
        try:
            if db_type == "oracle":
                with self.get_oracle_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 'Connection Successful' FROM DUAL")
                        result = cur.fetchone()
                        logger.info(f"Oracle Pre-check: {result[0]}")
                        return True
            elif db_type == "postgres":
                with self.get_postgres_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 'Connection Successful'")
                        result = cur.fetchone()
                        logger.info(f"PostgreSQL Pre-check: {result[0]}")
                        return True
        except Exception as e:
            logger.error(f"Pre-check failed for {db_type}: {e}")
            return False
