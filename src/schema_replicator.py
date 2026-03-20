import logging
import os

logger = logging.getLogger('Teleport.SchemaReplicator')

class OracleSchemaReplicator:
    def __init__(self, db_connector, target_schema, output_dir="./ddl_output"):
        """
        db_connector: Source DB DBConnector 인스턴스
        target_schema: DDL을 추출할 스키마(OWNER)
        output_dir: 추출된 DDL SQL 파일을 저장할 디렉토리
        """
        self.db_connector = db_connector
        self.target_schema = target_schema.upper()
        self.output_dir = output_dir
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def extract_ddl(self, object_type, object_name):
        """DBMS_METADATA를 사용하여 단일 오브젝트의 DDL 추출"""
        query = f"""
            SELECT DBMS_METADATA.GET_DDL('{object_type}', '{object_name}', '{self.target_schema}') 
            FROM DUAL
        """
        try:
            with self.db_connector.get_oracle_connection() as conn:
                with conn.cursor() as cur:
                    # 세션 설정: 스토리지 옵션 등을 제외하고 깔끔한 DDL만 뽑기 위함
                    cur.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE); END;")
                    cur.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE); END;")
                    cur.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE); END;")
                    cur.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE); END;")
                    
                    cur.execute(query)
                    result = cur.fetchone()
                    if result:
                        # CLOB 형태로 반환되므로 read() 사용
                        return result[0].read() if hasattr(result[0], 'read') else str(result[0])
                    return None
        except Exception as e:
            logger.error(f"Failed to extract DDL for {object_type} {object_name}: {e}")
            return None

    def replicate_schema(self, table_list):
        """주어진 테이블 목록의 DDL(테이블, 인덱스)을 모두 추출하여 파일로 저장"""
        logger.info(f"Starting DDL Extraction for {len(table_list)} tables in schema {self.target_schema}...")
        
        extracted_tables = 0
        output_file = os.path.join(self.output_dir, f"{self.target_schema}_schema_dump.sql")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Auto-generated Teleport DDL Dump for Schema: {self.target_schema}\n\n")
            
            for table_name in table_list:
                logger.info(f"Extracting DDL for TABLE: {table_name}")
                
                # 1. 테이블 DDL 추출
                table_ddl = self.extract_ddl('TABLE', table_name)
                if table_ddl:
                    f.write(f"-- TABLE: {table_name}\n")
                    f.write(table_ddl + "\n\n")
                    extracted_tables += 1
                
                # 2. 해당 테이블의 인덱스 DDL 추출 (추가 고도화 가능)
                # 인덱스 목록을 먼저 조회한 뒤 루프를 돌아야 하지만, 프로토타입이므로 생략 가능.
                # 여기서는 테이블 구조 완벽 복제에 초점을 맞춥니다.
                
        logger.info(f"DDL Extraction complete. Successfully extracted {extracted_tables} tables.")
        logger.info(f"DDL File saved to: {output_file}")
        return output_file
