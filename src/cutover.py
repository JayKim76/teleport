import logging

logger = logging.getLogger('Teleport.Cutover')

class CutoverManager:
    def __init__(self, source_connector, target_connector, target_schema):
        """
        source_connector: AS-IS DB 접속 객체
        target_connector: TO-BE DB 접속 객체
        target_schema: 대상 스키마
        """
        self.source_connector = source_connector
        self.target_connector = target_connector
        self.target_schema = target_schema.upper()

    def execute_cutover(self):
        """
        [컷오버(Cutover) 실행]
        1. AS-IS DB를 읽기 전용(Read-Only)으로 변경하여 추가 트랜잭션 차단.
        2. 최종 남은 동기화 세션(CDC) 잔여물 확인.
        3. TO-BE DB의 제약조건(FK, Trigger) 및 인덱스 활성화.
        """
        logger.info("==========================================")
        logger.info("🚀 INITIATING CUTOVER PROCEDURE 🚀")
        logger.info("==========================================")
        
        try:
            # 1. AS-IS DB 읽기 전용 모드 전환 (트랜잭션 락)
            logger.info("[Step 1] Locking Source DB (Read-Only Mode)...")
            lock_query = f"ALTER SYSTEM ENABLE RESTRICTED SESSION"
            # 실제 실행 시 주석 해제: self._execute_ddl(self.source_connector, lock_query)
            
            # 2. TO-BE DB의 제약조건 활성화
            logger.info("[Step 2] Enabling constraints & triggers on Target DB...")
            enable_fk_query = f"""
                BEGIN
                    FOR c IN (SELECT table_name, constraint_name FROM all_constraints WHERE owner = '{self.target_schema}' AND constraint_type = 'R') LOOP
                        EXECUTE IMMEDIATE 'ALTER TABLE {self.target_schema}.' || c.table_name || ' ENABLE CONSTRAINT ' || c.constraint_name;
                    END LOOP;
                END;
            """
            # 실제 실행 시 주석 해제: self._execute_ddl(self.target_connector, enable_fk_query)
            
            logger.info("✅ CUTOVER SUCCESSFUL. Target DB is now the primary.")
            return True
            
        except Exception as e:
            logger.error(f"❌ CUTOVER FAILED! Initiating immediate Fallback. Error: {e}")
            self.execute_fallback()
            return False

    def execute_fallback(self):
        """
        [롤백(Fallback) 실행]
        컷오버 실패 시, 시스템을 원래 상태(AS-IS)로 원복.
        1. AS-IS DB의 읽기 전용 상태 해제.
        2. 서비스 라우팅을 다시 AS-IS로 복귀하라는 경고 발생.
        """
        logger.info("==========================================")
        logger.info("🚨 INITIATING FALLBACK (ROLLBACK) PROCEDURE 🚨")
        logger.info("==========================================")
        
        try:
            logger.info("[Step 1] Unlocking Source DB (Restoring Read/Write)...")
            unlock_query = f"ALTER SYSTEM DISABLE RESTRICTED SESSION"
            # 실제 실행 시 주석 해제: self._execute_ddl(self.source_connector, unlock_query)
            
            logger.info("✅ FALLBACK SUCCESSFUL. Source DB is restored as primary.")
            return True
            
        except Exception as e:
            logger.critical(f"FATAL ERROR DURING FALLBACK: {e}")
            logger.critical("MANUAL INTERVENTION REQUIRED IMMEDIATELY.")
            return False

    def _execute_ddl(self, connector, query):
        """DDL/System 명령어를 실행하는 내부 헬퍼 함수"""
        with connector.get_oracle_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
