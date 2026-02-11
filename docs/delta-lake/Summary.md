# Delta Lake 주요 기능 요약

*(3.2 / 3.3 / 4.0)*

> 이 문서는 **Delta Lake 3.2, 3.3, 4.0**에서 새로 도입되었거나 의미 있게 변경된 기능 중,
> 운영 및 아키텍처 관점에서 중요하다고 판단한 항목들을 **간단히 정리**한 문서이다.

---

## Delta Lake 3.2

---

### VACUUM Inventory

**한 줄 요약**
스토리지 전체 디렉터리 listing 없이, 사전에 수집한 Inventory를 이용해 VACUUM 성능을 개선하는 기능.

**기능 요약**
기존 `VACUUM`은 객체 스토리지의 전체 파일 목록을 직접 조회해야 했기 때문에 대규모 테이블에서는 비용과 시간이 컸다.
VACUUM Inventory는 스토리지 파일 목록을 **사전에 Inventory 테이블로 수집**하고, VACUUM 시점에는 Delta 로그와 Inventory를 비교함으로써 full listing을 피한다.
대형 테이블 정리 작업의 **성능과 예측 가능성**을 개선한다.

**사용 예시**

```sql
VACUUM my_table
USING INVENTORY my_inventory_table
RETAIN 168 HOURS;
```

**주의사항**

* Inventory 테이블이 최신 상태가 아니면 삭제 정확도가 떨어질 수 있음
* Inventory 생성/유지에 별도의 운영 비용 필요

**상태**
GA (정식)

---

### Row Tracking

**한 줄 요약**
Delta 테이블의 각 row를 고유하게 식별하고 변경 이력을 추적할 수 있게 하는 기능.

**기능 요약**
Row Tracking은 각 행에 대해 내부적으로 `row_id`와 `row_commit_version`을 관리하여,
row-level 변경 추적을 가능하게 한다.
Deletion Vector, CDC, 고급 MERGE 시나리오 등 **고급 기능의 기반이 되는 핵심 기능**이다.

**사용 예시**

```sql
ALTER TABLE my_table
SET TBLPROPERTIES (
  delta.enableRowTracking = true
);
```

**주의사항**

* 테이블 프로토콜 업그레이드 발생
* Row Tracking을 지원하지 않는 엔진은 접근 불가
* 메타데이터 비용 증가 가능

**상태**
GA (정식)

---

### New SQL configurations for Delta Log cache

**한 줄 요약**
Delta Transaction Log 접근 성능을 제어하기 위한 SQL 레벨 캐시 설정 옵션 추가.

**기능 요약**
대규모 테이블에서는 `_delta_log`의 JSON/Checkpoint 파일을 반복적으로 읽는 비용이 커질 수 있다.
3.2부터는 Delta Log 캐시 동작을 SQL 설정으로 제어할 수 있어, 메타데이터 접근 비용을 줄일 수 있다.

**사용 예시**

```sql
-- SET spark.databricks.delta.log.cache.enabled = true;
```

**주의사항**

* Driver 메모리 사용량 증가 가능
* 환경별 튜닝 필요

**상태**
GA (정식)

---

### VACUUM metrics 개선

**한 줄 요약**
VACUUM 동작을 더 잘 관찰할 수 있도록 내부 메트릭이 확장됨.

**기능 요약**
VACUUM 실행 시 삭제 대상 파일 수, 처리 단계별 정보 등 추가 메트릭이 제공되어
정리 작업의 효과와 비용을 보다 명확하게 파악할 수 있다.

**상태**
GA (정식)

---

### Type Widening

**한 줄 요약**
데이터 파일 재작성 없이 컬럼 타입을 더 넓은 타입으로 변경할 수 있는 스키마 진화 기능.

**기능 요약**
`INT → BIGINT`와 같이 안전한 방향의 타입 변경 시, 기존 Parquet 파일을 재작성하지 않고
논리적 스키마만 확장한다.
대규모 테이블에서 타입 확장 비용을 크게 줄일 수 있다.

**사용 예시**

```sql
ALTER TABLE my_table
ALTER COLUMN value TYPE BIGINT;
```

**주의사항**

* 허용되는 타입 변환 조합이 제한적
* 3.2 기준 Spark 중심 기능 (엔진 호환성 주의)

**상태**
Preview (3.2 기준)

---

## Delta Lake 3.3

---

### Identity Columns

**한 줄 요약**
INSERT 시 자동으로 고유 값을 생성하는 컬럼을 지원하는 기능.

**기능 요약**
Identity Column은 사용자가 값을 명시하지 않아도 Delta Lake가 자동으로 고유 값을 생성한다.
Surrogate key, 내부 식별자 생성 등을 단순화한다.

**사용 예시**

```sql
CREATE TABLE my_table (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  value STRING
);
```

**주의사항**

* 분산 환경 특성상 값이 연속적이지 않을 수 있음

**상태**
GA (정식)

---

### VACUUM LITE

**한 줄 요약**
Delta Transaction Log만을 이용해 수행되는 경량 VACUUM 방식.

**기능 요약**
스토리지 전체 listing 없이 `_delta_log`의 AddFile/RemoveFile 정보만으로
논리적으로 사용되지 않는 파일을 정리한다.
주기적인 VACUUM 작업에 적합하며 매우 빠르다.

**사용 예시**

```sql
VACUUM my_table RETAIN 168 HOURS;
```

※ 조건 충족 시 내부적으로 LITE 방식 자동 선택

**주의사항**

* Delta 로그에 등장하지 않은 파일은 삭제 불가
* 외부에서 생성된 orphan 파일 정리에는 부적합

**상태**
GA (정식)

---

## Delta Lake 4.0

---

### Catalog-managed tables

**한 줄 요약**
Delta 커밋을 파일 시스템이 아닌 Catalog가 중개하는 새로운 트랜잭션 모델.

**기능 요약**
기존에는 클라이언트가 직접 `_delta_log`에 커밋을 기록했으나,
Catalog-managed tables에서는 Catalog가 커밋을 broker하여
동시성 제어 및 커밋 순서를 관리한다.
멀티 엔진 write와 중앙 통제를 위한 구조적 변화이다.

**주의사항**

* 큰 아키텍처 변화
* 프로토콜 및 동작 방식 변경 가능

**상태**
Preview

---

### Delta Connect

**한 줄 요약**
Spark Connect 아키텍처에서 Delta Lake를 사용할 수 있게 하는 기능.

**기능 요약**
클라이언트와 Spark 실행 환경을 분리하는 Spark Connect 모델에서
Delta Lake 연산을 서버 측 기능으로 제공한다.
Delta는 더 이상 클라이언트 라이브러리가 아니라 **중앙 서비스 역할**을 수행한다.

**주의사항**

* 일부 Delta 기능 미지원 가능
* 성숙도 낮음

**상태**
Preview

---

### Variant data type

**한 줄 요약**
반정형 데이터를 스키마-온-리드 방식으로 저장하는 Delta Lake의 1급 데이터 타입.

**기능 요약**
Variant 타입은 JSON과 같은 구조가 가변적인 데이터를 그대로 저장하고,
읽기 시 필요한 필드만 해석할 수 있도록 한다.
기존 JSON STRING 대비 타입 안정성과 엔진 간 일관성이 크게 개선된다.

**사용 예시**

```sql
CREATE TABLE my_table (
  payload VARIANT
);
```

**주의사항**

* 쿼리 패턴에 따라 성능 고려 필요
* 자주 쓰는 필드는 Shredded Variant 고려

**상태**
GA (4.0.0부터 정식)

---

## 요약

> **Delta Lake 3.2–3.3은 운영 효율과 성능 개선에 초점이 맞춰져 있고,
> Delta Lake 4.0은 아키텍처와 데이터 모델의 확장을 본격적으로 시작한 릴리스다.**