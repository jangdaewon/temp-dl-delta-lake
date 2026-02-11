https://delta.io/blog/delta-lake-3-3/

https://github.com/delta-io/delta/releases/tag/v3.3.0


# Identity Columns — 자동 고유값 생성 기능
Identity Columns는 Delta가 테이블에 자동 증가 식별자를 생성해주는 기능으로, 기본 키가 필요하거나 MERGE/UPSERT 등 행 무결성이 중요한 상황에서 편의성과 일관성을 크게 개선한다.

```sql
CREATE TABLE my_table (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  data STRING
);
```

## 📌 Identity vs Surrogate Key 생성과의 차이

| 항목       | Identity Column | UUID/Hash 등 |
| -------- | --------------- | ----------- |
| 자동 생성 여부 | ✅               | 가능(사용자 생성)  |
| 충돌 리스크   | ❌               | ❌(낮지만 가능)   |
| 순차 증가    | 가능              | 불가          |
| ACID 일관성 | 보장              | 보장 대상이 아님   |



# VACUUM LITE (Delta Lake 3.3+)
**기존 VACUUM의 문제**

* 기존 `VACUUM`은 **스토리지 디렉터리 전체를 listing** 해서
  → “현재 테이블 메타데이터에서 참조되지 않는 파일”을 찾음
* 대규모 테이블 / 객체 스토리지(S3·MinIO·GCS 등)에서는
  **디렉터리 listing 자체가 매우 비싸고 느림**

---

**VACUUM LITE의 핵심 아이디어**

* **스토리지 listing을 하지 않는다**
* 대신 **Delta Transaction Log (`_delta_log`)만을 기준**으로 판단

| 구분            | VACUUM Inventory  | VACUUM LITE               |
| ------------- | ----------------- | ------------------------- |
| 기준 데이터        | **Inventory 테이블** | **Delta Transaction Log** |
| 스토리지 listing  | ❌ (사전 수집)         | ❌                         |
| 실제 스토리지 상태 반영 | ✅                 | ❌                         |
| 로그 외부 파일 삭제   | ✅                 | ❌                         |
| 운영 복잡도        | 높음                | 매우 낮음                     |
| 성능            | 빠름                | 매우 빠름                     |
| 주 사용 시나리오     | 대규모 레거시 정리        | 주기적 VACUUM                |

## 2️⃣ VACUUM LITE가 자동으로 활성화되는 조건

다음 조건을 **모두 만족**하면 Delta가 **VACUUM LITE 경로**를 선택합니다.

### ✅ 필수 조건

* **Delta Lake 3.3 이상**
* **Retention 기간이 명시적** (`RETAIN n HOURS`)
* **Safety check 통과**

  * `delta.deletedFileRetentionDuration` 준수
  * Time Travel 안전성 보장

### 🚫 LITE가 비활성화되는 경우

* `VACUUM table` (retain 미지정)
* `VACUUM table RETAIN 0 HOURS`
* `spark.databricks.delta.retentionDurationCheck.enabled = false`
* Inventory 옵션을 명시적으로 사용한 경우