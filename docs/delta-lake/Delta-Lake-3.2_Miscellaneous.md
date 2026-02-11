https://groups.google.com/g/delta-users/c/e6_mXVSsY8c/m/z472UvM3BAAJ

# VACUUM Inventory
VACUUM Inventory 지원은, 대형 Delta 테이블에서 VACUUM 시 전체 스토리지를 스캔하지 않고 Delta log 기반 인벤토리만 사용해, 유지보수 시간을 획기적으로 줄여주는 기능이다.

## 4️⃣ 효과 (왜 중요한가)

### ✅ VACUUM 속도 대폭 개선
* 디렉터리 listing 제거
* 특히 MinIO / S3 계열에서 체감 큼

### ✅ 스토리지 메타데이터 부하 감소
* LIST 요청 폭발 방지
* 오브젝트 스토리지 안정성 향상

### ✅ 대형 테이블에서 정기 VACUUM 가능
* “주말에만 돌리던 작업” → **일상 운영 작업**


## (1) 테이블/세션에서 Inventory 사용 활성화

보통은 **VACUUM 실행 시 옵션으로 지정**합니다.

```sql
VACUUM <db>.<table>
USING INVENTORY;
```

또는 Spark SQL 옵션으로 명시:

```sql
SET spark.databricks.delta.vacuum.useInventory=true;
```

---

# Row Tracking
**Row Tracking 쓰기 지원은, Delta Lake가 각 행에 영구적인 논리 ID를 부여하고 DML 시 이를 함께 기록하게 함으로써, CDC·감사·idempotent 처리 같은 행 단위 데이터 관리의 기반을 제공하는 기능이다.**
> **각 행(row)에 전 테이블 생애 동안 유지되는 ‘논리적 ID’를 부여하는 기능**

* Row Tracking은
    * 일부 읽기/메타 구조만 존재
    * **실제 DML(write)에서 완전 활용 불가**

# New SQL configurations for Delta Log cache
New SQL configurations to specify Delta Log cache size (spark.databricks.delta.delta.log.cacheSize) and retention duration (spark.databricks.delta.delta.log.cacheRetentionMinutes)


# VACUUM metrics 개선 (기존에는 성공 여부만 확인 가능), Spark UI / 로그에서 다음 메트릭 확인 가능
* 삭제된 파일 수
* 삭제된 바이트 수
* 스캔 대상 파일 수
* 실행 시간