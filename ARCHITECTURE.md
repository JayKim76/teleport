# Project Teleport: Oracle Migration Solution Architecture & Roadmap

## 1. 마이그레이션 워크플로우 (Workflow) 및 누락 요소 점검

보스께서 제시해주신 5단계는 마이그레이션의 훌륭한 핵심 뼈대입니다. 실제 엔터프라이즈 오라클 이관 프로젝트의 무결성을 위해 **[스키마/오브젝트 변환]**, **[사전 검증(Pre-Check)]**, **[롤백(Fallback) 계획]** 단계를 추가하여 워크플로우를 보강했습니다.

### 🔄 보강된 Teleport 8단계 워크플로우
1. **[추가] 사전 검증 및 권한 체크 (Pre-flight Check)**
   - 방화벽, 포트, 최소 필요 권한(DBA 권한 등) 자동 점검.
2. **AS-IS / TO-BE 서버 접속 (Connection Setup)**
   - Source(Oracle)와 Target DB의 안정적인 Connection Pool 및 터널링 설정.
3. **서버 및 오라클 환경 분석 (Assessment & Discovery)**
   - DB 용량, 테이블 개수, 데이터 타입, LOB/CLOB 데이터 존재 여부, Character Set 파악.
   - 종속성(Dependencies), 제약조건(Constraints) 및 파티션(Partition) 구조 분석.
4. **[추가] 스키마 및 오브젝트 변환 (Schema & Object Translation)**
   - DDL 추출 및 Target DB 문법에 맞는 DDL 자동 변환 생성.
   - PL/SQL (프로시저, 함수, 트리거) 호환성 분석 및 변환 리포트 제공.
5. **마이그레이션 계획 수립 (Migration Planning)**
   - 병렬 처리(Parallel) 쓰레드 수, 청크(Chunk) 사이즈 분할 기준 설정.
   - 테이블별 이관 순서(종속성 기반) 및 다운타임(Downtime) 산정.
6. **마이그레이션 실행 (Execution: Initial Load + CDC)**
   - 초기 풀 적재(Full Load) 및 필요 시 변경 데이터 캡처(CDC)를 통한 실시간 동기화.
7. **데이터 검증 및 통계정보 확인 (Validation & Statistics)**
   - Row Count 검증, 해시섬(Hash Sum) 혹은 샘플링 데이터 일치 여부 검증.
   - Target DB 통계정보 갱신(Analyze) 및 인덱스 재빌드.
8. **[추가] 컷오버 및 롤백 플랜 (Cutover & Fallback)**
   - 최종 전환(Cutover) 및 실패 시 원복을 위한 역동기화(Reverse Sync) 아키텍처.

---

## 2. 각 단계별 개발 필요 사항 (Development Requirements)

Teleport 솔루션을 구현하기 위해 개발되어야 할 핵심 모듈들을 정리합니다.

### 모듈 1: Connection & Pre-Check Manager (접속 및 사전점검)
- **DB Driver Integration**: Oracle(cx_Oracle/oracledb), PostgreSQL(psycopg2), MySQL(PyMySQL) 등의 네이티브 드라이버 연동.
- **Connection Test API**: 응답 시간, 권한 유효성(GRANT SELECT ANY DICTIONARY 등) 테스트 로직.

### 모듈 2: Discovery & Assessment Engine (환경 분석 엔진)
- **Metadata Extractor**: 오라클 시스템 카탈로그(`DBA_TABLES`, `DBA_TAB_COLUMNS`, `DBA_OBJECTS` 등) 쿼리 모듈.
- **Data Profiling**: 테이블별 용량, Row 수, LOB 데이터 존재 유무를 분석하여 난이도를 스코어링하는 기능.

### 모듈 3: Schema Translator (스키마 변환기)
- **Data Type Mapper**: Oracle의 `NUMBER`, `VARCHAR2`, `DATE`, `CLOB` 등을 Target DB의 타입으로 1:1 맵핑하는 룰셋(Rule-set) 엔진.
- **DDL Generator**: 변환된 맵핑 룰을 바탕으로 Target DB에서 실행 가능한 `CREATE TABLE`, `ALTER TABLE` 구문을 생성.

### 모듈 4: Teleport Data Pump (고속 이관 엔진) - 핵심 코어 🚀
- **Chunk Splitter**: 대용량 테이블을 PK나 ROWID, 혹은 파티션 단위로 잘게 쪼개는 알고리즘 (메모리 오버플로우 방지).
- **Parallel Worker**: 멀티 프로세스/쓰레드를 활용하여 데이터를 동시에 `SELECT(Oracle) -> INSERT/COPY(Target)` 하는 파이프라인.
- **Resumable Control**: 네트워크 단절 시 실패한 Chunk부터 재시작(Resume) 할 수 있는 체크포인트 관리 기능.

### 모듈 5: Data Validator (정합성 검증기)
- **Fast Count Check**: 이관 전후의 `COUNT(*)` 비교 스크립트 자동 실행.
- **Data Hash Check (선택)**: 랜덤 샘플링 데이터를 해싱하여 값이 정확히 일치하는지 비교하는 정밀 검증 로직.

### 모듈 6: Dashboard & Reporter (UI/UX 및 리포트)
- **CLI or Web UI**: 진행률(Progress Bar), 성공/실패 로그, 예상 남은 시간을 직관적으로 보여주는 인터페이스.
- **Audit Report**: 이관 완료 후 "환경 분석 -> 스키마 변환 성공률 -> 데이터 검증 결과"를 담은 PDF/HTML 보고서 자동 생성 기능.

---
*개발 스택(Python/Go/Java 등)과 타겟 DB가 정해지면, 이 아키텍처를 바탕으로 `Teleport Data Pump` 엔진의 첫 번째 프로토타입 코드를 작성할 수 있습니다.*