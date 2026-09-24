# คู่มือ: แยก DAG bundle ให้ APxxxx-YYYYY และ APzzzz-WWWWW (Airflow 3 + Helm)

> **ชื่อในคู่มือนี้เป็นตัวแทน (placeholder)** ให้แทนด้วยค่าจริงของระบบ โครงสร้าง path ของไฟล์ DAG เป็นแบบนี้:
>
> ```
> /opt/airflow/dags/<company_name>/APxxxx-YYYYY/dev_<company_name>_apxxxx_yyyyy_workflow_group2_corp.py
>                   └ ชื่อบริษัท      └ รหัส-ชื่อ app   └ env_บริษัท_รหัส_ชื่อ app_ชื่องาน
> ```
>
> | placeholder | ความหมาย |
> |---|---|
> | `<company_name>` | folder ชื่อบริษัทใต้ `/opt/airflow/dags` |
> | `APxxxx-YYYYY` | app ที่ใช้เวลา parse มาก (แยกเป็น bundle `heavy`) |
> | `APzzzz-WWWWW` | app ที่ใช้ทดลอง (แยกเป็น bundle `pilot`) |
>
> ชื่อ bundle (`heavy`, `pilot`) ตั้งเป็นอะไรก็ได้ เช่นใช้รหัส app ตัวพิมพ์เล็ก ตัวเลข (จำนวน DAG, เวลา parse) มาจาก export ตัวอย่าง

เป้าหมาย: ให้ app ที่ใช้เวลา parse มาก (APxxxx-YYYYY) และ app ที่ใช้เป็นกลุ่มทดลอง (APzzzz-WWWWW) มี dag-processor ของตัวเอง จะได้**ไม่ต้องรอคิว parse รวมกับ app อื่น**

ตัวเลขที่ใช้วางแผนมาจากรายงาน `report/dag_processor_report.html` (export วันที่ 2026-09-23 18:14 เฉพาะ DAG ที่ active)

> ⚠ **สมมติฐานที่ต้องตรวจก่อนทำ**
> - ติดตั้งด้วย chart ทางการ `apache-airflow/airflow`
> - DAG ถูก mount จาก blob storage มาที่ `/opt/airflow/dags`
> - ชื่อ key และ path ของ template ใน chart, flag `airflow dag-processor --bundle-name`, รูปแบบ `dag_bundle_config_list` และพฤติกรรมตอนย้าย DAG ข้าม bundle อ้างอิงจากความรู้เรื่อง Airflow 3 **ยังไม่ได้ทดสอบกับเวอร์ชันที่ใช้จริง**
>
> ขั้นตอนที่ 0 และ 1 มีไว้ตรวจเรื่องเหล่านี้ ถ้าผลไม่ตรงกับคู่มือ ให้หยุดก่อน

---

## ภาพรวม

| bundle | path | dag-processor | parsing_processes | DAG (active) | เวลา parse รวม | รอบที่คาดไว้* |
|---|---|---|---|---|---|---|
| `dags-folder` (เดิม) | `/opt/airflow/dags` (ยกเว้น APxxxx-YYYYY, APzzzz-WWWWW) | deployment เดิมของ chart | 16 | ~1,070 | ~12.5 ชม. | ~47 นาที |
| `heavy` | `/opt/airflow/dags/<company_name>/APxxxx-YYYYY` | deployment ใหม่ | 16 | 426 | 8.45 ชม. | ~32 นาที |
| `pilot` | `/opt/airflow/dags/<company_name>/APzzzz-WWWWW` | deployment ใหม่ | 4 | 30 | 0.60 ชม. | ~9 นาที |

\* รอบที่คาดไว้ = เวลา parse รวม ÷ parsing_processes สมมติว่าแต่ละ process ไม่ได้แย่งระบบภายนอกตัวเดียวกัน ตอนนี้รอบที่วัดได้อยู่ที่ประมาณ 75–92 นาที ซึ่งเท่ากับที่ใช้ 16 processes ไม่ใช่ 32 (ดูหัวข้อ "dag-processor 2 replicas")

- **คงชื่อ `dags-folder` ไว้:** DAG ส่วนใหญ่จะไม่ต้องย้าย bundle มีแค่ APxxxx-YYYYY (426) กับ APzzzz-WWWWW (30) ที่ย้าย
- **APzzzz-WWWWW ใช้เป็นกลุ่มทดลอง:** median parse 88 วินาที มี 30 DAG เล็กพอจะเห็นผลชัดว่าเวลารอเกิดจากคิวรวมหรือจาก app เอง

## ตั้ง bundle อย่างเดียวไม่ทำให้เร็วขึ้น: ต้องมี dag-processor เฉพาะ bundle

bundle เป็นแค่**การแบ่งกลุ่มเชิงตรรกะ** ถ้า dag-processor ไม่ได้ระบุ `--bundle-name` มันจะ parse **ทุก bundle** ต่อกันในรอบเดียว ด้วย `parsing_processes` ชุดเดิม รอบจึงยังยาวเท่าเดิม (≈ เวลา parse รวม ÷ processes) สิ่งที่เปลี่ยนมีแค่ค่า `bundle_name` ของ DAG

ความเร็วมาจากการให้ bundle มี **dag-processor (pod) ของตัวเอง** DAG ของ bundle นั้นจะไม่ต้องรอคิวรวมกับ app อื่น

```
ก่อน (หรือตั้ง bundle แต่ไม่ได้แยก processor)      หลัง (แยก processor)
┌─────────────────────────────┐                 ┌───────────────────────────────────────┐
│ airflow dag-processor       │                 │ airflow dag-processor                 │
│  → dags-folder + pilot      │                 │   --bundle-name dags-folder  (16 proc)│
│  (คิวเดียว ~75–90 นาที)       │                 ├───────────────────────────────────────┤
└─────────────────────────────┘                 │ airflow dag-processor                 │
                                                │   --bundle-name pilot         (4 proc)│
                                                │  (คิวของ pilot เอง ~9 นาที)             │
                                                └───────────────────────────────────────┘
```

ต้องมี**ครบทั้ง 4 ข้อ**:

| # | สิ่งที่ต้องมี | ขั้นตอน | ถ้าขาด |
|---|---|---|---|
| 1 | `dag_bundle_config_list` ค่าเดียวกันใน**ทุก component** (scheduler, api-server, worker, triggerer, dag-processor) | A2 / B2 | worker หาไฟล์ของ DAG ใน `pilot` ไม่เจอตอนรัน task |
| 2 | dag-processor เดิมรัน `airflow dag-processor --bundle-name dags-folder` | A3 / B2 | processor เดิมยัง parse `pilot` ด้วย ไม่ได้แยกจริง |
| 3 | deployment **แยก** รัน `airflow dag-processor --bundle-name pilot` พร้อม `PARSING_PROCESSES` ของตัวเอง | A4 / B3 | ไม่มีใคร parse `pilot` โดยเฉพาะ |
| 4 | `.airflowignore` ตัด `<company_name>/APzzzz-WWWWW/` ออกจาก `dags-folder` | A5 / B4 | ไฟล์ถูก parse **2 รอบ** (`dags-folder` เริ่มที่ `/opt/airflow/dags` จึงเห็น folder ย่อยด้วย) ช้าลง และ `bundle_name` สลับไปมา |

**ตรวจว่าตอนนี้ครบหรือยัง** (ใช้ได้ทั้งก่อนเริ่มและหลังตั้ง bundle ไปแล้ว):

```sh
NS=airflow
# ข้อ 2, 3: แต่ละ processor ผูกกับ bundle ไหน (ต้องเห็น --bundle-name ทุกตัว)
kubectl -n $NS get deploy -l 'component in (dag-processor,dag-processor-pilot)' \
  -o custom-columns='NAME:.metadata.name,REPLICAS:.spec.replicas,ARGS:.spec.template.spec.containers[0].args'

# ข้อ 1: ทุก component เห็น bundle config เดียวกัน
for c in scheduler api-server triggerer worker dag-processor; do
  pod=$(kubectl -n $NS get pods -l component=$c -o name | head -1)
  [ -n "$pod" ] && echo "== $c" && kubectl -n $NS exec "$pod" -- \
    airflow config get-value dag_processor dag_bundle_config_list
done

# ข้อ 4
kubectl -n $NS exec deploy/airflow-dag-processor -- cat /opt/airflow/dags/.airflowignore
```

```sql
-- DAG ของ APzzzz-WWWWW ต้องอยู่ pilot ทั้งหมด ถ้ายังมีแถว dags-folder แปลว่าข้อ 2 หรือ 4 ขาด
SELECT bundle_name, is_stale, count(*) FROM dag
WHERE fileloc LIKE '%/<company_name>/APzzzz-WWWWW/%' GROUP BY 1, 2;
```

| อาการ | สาเหตุที่น่าจะเป็น |
|---|---|
| ตั้ง bundle แล้ว แต่ `oldest_parse_min` ของ `pilot` ยัง ~75–90 นาที | ไม่มี processor เฉพาะ `pilot` (ข้อ 2 หรือ 3 ขาด) |
| `bundle_name` ของ DAG เดียวกันสลับระหว่าง `dags-folder` กับ `pilot` | `.airflowignore` ยังไม่ตัด folder (ข้อ 4) |
| DAG ของ `pilot` เป็น stale ทั้งที่ไฟล์ยังอยู่ | ไม่มี processor ไหน parse `pilot` หรือ path ใน `kwargs` ผิด |
| task ของ DAG ใน `pilot` fail ว่าหาไฟล์/bundle ไม่เจอ | worker ไม่มี `dag_bundle_config_list` (ข้อ 1) |

> **ข้อจำกัด:** processor เฉพาะ bundle ลดเวลา**รอคิว** ไม่ได้ลดเวลา parse ของแต่ละไฟล์ ไฟล์ที่ใช้ 90 วินาทีก็ยังใช้ 90 วินาที และถ้าโค้ด top-level ของ DAG เรียกระบบภายนอกตัวเดียวกัน (DB, API, Vault) ทั้งสอง processor ยังแย่งกันอยู่

## dag-processor 2 replicas: ช่วยจริงหรือ parse ซ้ำ

ตอนนี้ deployment `airflow-dag-processor` มี `replicas = 2` (16 processes ต่อ pod) ตัวเลขจาก export ตัวอย่าง (DAG ที่ active 1,528 ตัว, เวลา parse รวม 21.5 ชม.):

| | รอบ |
|---|---|
| รอบที่คาดไว้ ถ้า 2 pods แบ่งงานกัน (32 processes) | 40 นาที |
| รอบที่คาดไว้ ถ้าได้ผลเท่า pod เดียว (16 processes) | 80 นาที |
| **รอบที่วัดได้** (p99 ของอายุ DAG / อัตรา parse ต่อชั่วโมง) | **92 / 75 นาที** |

รอบที่วัดได้ตรงกับ 16 processes แปลว่า replica ที่ 2 ไม่ได้ทำให้รอบสั้นลงครึ่งหนึ่ง สาเหตุที่เป็นไปได้ (**ยังไม่ได้ยืนยัน** เพราะ export ไม่บอกว่า pod ไหนเป็นคน parse):

| สมมติฐาน | ตรวจอย่างไร (A0) | ถ้าใช่ |
|---|---|---|
| **H1: 2 pods parse ไฟล์ชุดเดียวกันซ้ำ** แต่ละ pod ทำครบทุกไฟล์เอง | ไฟล์เดียวกันขึ้นใน log ของทั้ง 2 pods ภายในรอบเดียวกัน | replica ที่ 2 ไม่ได้ช่วยให้เร็วขึ้น เพิ่มแค่งานซ้ำ, CPU และจำนวน version ของ DAG ที่ non-deterministic ให้ตั้ง `replicas = 1` แล้วใช้ CPU ที่เหลือไปทำ processor ของ bundle ใหม่แทน |
| **H2: แบ่งงานกัน แต่ CPU ไม่พอ** CPU limit ของ pod หรือ node น้อยกว่า 16 core ต่อ pod | ดู `resources` ของ container และ `kubectl top pod` ว่าใช้ CPU เต็ม limit ตลอด | คอขวดคือ CPU ถ้าแยก bundle โดยไม่เพิ่ม CPU รอบจะไม่สั้นลง ต้องเพิ่ม `resources` หรือ node ก่อน |

**ผลกับรายงาน (`generate-report.sh`)**
- ค่าเริ่มต้น `PARSING_PROCESSES=32` คิดว่า 2 pods แบ่งงานกัน ถ้าเป็น H1 รอบที่คาดไว้ในรายงานจะเร็วกว่าจริงครึ่งหนึ่ง ให้สร้างอีกรายงานด้วย `PARSING_PROCESSES=16 ./generate-report.sh` เทียบกัน
- **รอบที่วัดได้, Freshness และ overdue ยังถูกต้อง** เพราะวัดจาก `last_parsed_time` ซึ่ง pod ไหน parse ก็อัปเดตค่านี้ ตัวเลขชุดนี้คือเวลาที่ผู้ใช้ต้องรอจริง
- `parse_seconds` เป็นค่าจาก pod ที่เขียนลง DB ล่าสุด ถ้า 2 pods อยู่ node เดียวกันแล้วแย่ง CPU กัน ค่าจะสูงกว่าเวลา parse จริงของไฟล์

**กติกาสำหรับ processor แยก bundle:** ตั้ง `replicas = 1` ทุกตัว (ส่วน A4 และ B3 ตั้งไว้แล้ว) ถ้าอยากให้เร็วขึ้น ให้เพิ่ม `PARSING_PROCESSES` และ CPU แทนการเพิ่ม replica จนกว่าจะยืนยันได้ว่าหลาย replica แบ่งงานกันจริง

## วิธีใช้คู่มือนี้

| ส่วน | เป้าหมาย | วิธี | ขอบเขต |
|---|---|---|---|
| **A: ทดสอบ bundle แบบ ad-hoc** | ยืนยันว่าการแยก bundle ทำให้รอบการ parse สั้นลงจริง | `kubectl` (ไม่ใช้ Helm) ย้อนกลับได้ด้วยคำสั่งไม่กี่บรรทัด | **APzzzz-WWWWW อย่างเดียว** บน **dev** |
| **B: ทำจริง** | แยก bundle แบบถาวร | Helm values + deployment ใน repo | APxxxx-YYYYY และ APzzzz-WWWWW |

ทำ **ส่วน A ก่อน** ถ้าผลไม่ชัด ยังไม่ต้องทำส่วน B

---

# ส่วน A: ทดสอบ bundle แบบ ad-hoc (kubectl, เฉพาะ APzzzz-WWWWW)

**แนวคิด:** ให้ APzzzz-WWWWW มี bundle `pilot` และ dag-processor ของตัวเอง แล้วดูว่า DAG ของ APzzzz-WWWWW ถูก parse ถี่ขึ้นจริงหรือไม่ ทุกอย่างทำด้วย `kubectl` บน dev และย้อนกลับได้

> ⚠ **ก่อนเริ่ม**
> - ทำบน **dev** เท่านั้น และแจ้งทีมที่ใช้ dev ขั้นตอนที่ A2 ทำให้ pod ของ Airflow restart
> - ตรวจก่อนว่า server ที่ทดสอบมี DAG ของ APzzzz-WWWWW จริง (ใน export ตัวอย่าง DAG ของ APzzzz-WWWWW เป็น env `sit` ทั้งหมด) ถ้าไม่มี ให้เลือก app อื่นที่มี DAG บน server นั้นเป็นกลุ่มทดลองแทน
> - ถ้าสร้าง bundle `pilot` ไปแล้ว ให้ใช้ตารางตรวจในหัวข้อ "ตั้ง bundle อย่างเดียวไม่ทำให้เร็วขึ้น" หาข้อที่ขาด แล้วทำเฉพาะขั้นตอนนั้น (A2–A5)
> - ถ้า Airflow ถูกจัดการด้วย GitOps (Argo CD, Flux) ให้ **pause การ sync** ก่อน ไม่อย่างนั้นการแก้ด้วย `kubectl` จะถูกเขียนทับ
> - `helm upgrade` ครั้งถัดไปจะเขียนทับ args ของ dag-processor แต่**จะไม่ลบ** env ที่เพิ่มด้วย `kubectl set env` ให้ทำ rollback (A7) ให้ครบก่อน `helm upgrade`
> - ชื่อ label (`component=...`) ชื่อ deployment และ container อ้างจาก chart ทางการ ให้ตรวจในขั้นตอน A0

## A0. ตรวจก่อนเริ่ม (อ่านอย่างเดียว)

```sh
NS=airflow
kubectl -n $NS exec deploy/airflow-dag-processor -- airflow dag-processor --help | grep -i bundle   # ต้องมี --bundle-name
kubectl -n $NS get deploy,sts -l 'component in (scheduler,api-server,triggerer,worker,dag-processor)'
kubectl -n $NS get deploy airflow-dag-processor \
  -o jsonpath='{range .spec.template.spec.containers[*]}{.name}{"\n"}{end}'                     # container แรกต้องเป็น dag-processor

# H1: 2 pods parse ไฟล์ซ้ำกันหรือไม่: ถ้าไฟล์เดียวกันขึ้นใน log ทั้ง 2 pods ห่างกันไม่ถึง 1 รอบ แปลว่าซ้ำ
FILE=dev_<company_name>_apzzzz_wwwww_workflow_group2_corp.py   # ไฟล์ DAG ใดก็ได้ใน APzzzz-WWWWW
for p in $(kubectl -n $NS get pods -l component=dag-processor -o name); do
  echo "== $p"; kubectl -n $NS logs "$p" --since=3h | grep -F "$FILE" | tail -3
done

# H2: CPU พอหรือไม่: pod อยู่ node ไหน, limit เท่าไร, ใช้จริงเท่าไร
kubectl -n $NS get pods -l component=dag-processor -o wide
kubectl -n $NS get deploy airflow-dag-processor -o jsonpath='{.spec.template.spec.containers[0].resources}'; echo
kubectl -n $NS top pod -l component=dag-processor
```

จดผล H1/H2 ไว้ก่อนทำ A1 (ดูหัวข้อ "dag-processor 2 replicas") ถ้าเป็น H2 ให้แก้เรื่อง CPU ก่อน เพราะการทดสอบ bundle จะเห็นผลไม่ชัด

**วัดค่าก่อนทดสอบ (baseline):** DAG ของ APzzzz-WWWWW ถูก parse ครั้งล่าสุดนานสุดกี่นาทีแล้ว

```sql
SELECT count(*) AS dags,
       round(extract(epoch FROM now() - min(last_parsed_time)) / 60) AS oldest_parse_min
FROM dag
WHERE NOT is_stale AND fileloc LIKE '%/<company_name>/APzzzz-WWWWW/%';
```

## A1. เตรียมตัวแปร และสำรองค่าเดิม

```sh
NS=airflow
BUNDLES='[{"name":"dags-folder","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/dags"}},{"name":"pilot","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/dags/<company_name>/APzzzz-WWWWW"}}]'
kubectl -n $NS get deploy airflow-dag-processor \
  -o jsonpath='{.spec.template.spec.containers[0].args}' > dag-processor-args-original.json
cat dag-processor-args-original.json
```

## A2. ใส่ bundle config ให้ทุก component (pod จะ restart)

```sh
kubectl -n $NS set env deploy,sts -l 'component in (scheduler,api-server,triggerer,worker,dag-processor)' \
  AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST="$BUNDLES"
```

ใช้ label เจาะจงเฉพาะ component ของ Airflow เพื่อไม่ให้ Redis หรือ PgBouncer restart ไปด้วย ต้องใส่ทุก component เพราะ worker ต้องหาไฟล์ของ DAG ใน bundle `pilot` ตอนรัน task

## A3. ให้ dag-processor เดิมดูแลเฉพาะ `dags-folder`

```sh
kubectl -n $NS patch deploy airflow-dag-processor --type json -p \
  '[{"op":"replace","path":"/spec/template/spec/containers/0/args",
     "value":["bash","-c","exec airflow dag-processor --bundle-name dags-folder"]}]'
```

## A4. สร้าง dag-processor ของ `pilot` (โคลนจาก deployment เดิม)

```sh
kubectl -n $NS get deploy airflow-dag-processor -o yaml | yq '
  del(.metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp,
      .metadata.generation, .metadata.managedFields, .metadata.annotations, .status) |
  .metadata.name = "airflow-dag-processor-pilot" |
  .metadata.labels.component = "dag-processor-pilot" |
  .spec.replicas = 1 |
  .spec.selector.matchLabels.component = "dag-processor-pilot" |
  .spec.template.metadata.labels.component = "dag-processor-pilot" |
  .spec.template.spec.containers[0].args = ["bash","-c","exec airflow dag-processor --bundle-name pilot"] |
  .spec.template.spec.containers[0].env += [{"name":"AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES","value":"4"}]
' | kubectl -n $NS apply -f -

kubectl -n $NS rollout status deploy/airflow-dag-processor-pilot
```

การเปลี่ยน label `component` ทำให้ deployment เดิมไม่นับ pod ของ `pilot` เป็นของตัวเอง

## A5. ตัด APzzzz-WWWWW ออกจาก `dags-folder` (`.airflowignore`)

เพิ่มบรรทัดนี้ใน `.airflowignore` ที่ root ของ blob (ถ้ามีไฟล์อยู่แล้ว ให้เพิ่ม ไม่ใช่เขียนทับ):

```
<company_name>/APzzzz-WWWWW/
```

ทำขั้นนี้**หลัง** A4 เพื่อให้ processor ของ `pilot` เริ่มรับ DAG ไปก่อน ช่วงสั้น ๆ ระหว่าง A4 กับ A5 ที่ทั้งสอง bundle parse ไฟล์เดียวกันอาจทำให้ `bundle_name` ของ DAG สลับไปมา ถือว่ารับได้ในการทดสอบบน dev

## A6. ตรวจและวัดผล (หลัง A5 ประมาณ 10–15 นาที)

```sql
-- 1) DAG ของ APzzzz-WWWWW ต้องย้ายมาอยู่ bundle "pilot" ครบ และไม่เป็น stale
SELECT bundle_name, is_stale, count(*) FROM dag
WHERE fileloc LIKE '%/<company_name>/APzzzz-WWWWW/%' GROUP BY 1, 2;

-- 2) รอบการ parse ของแต่ละ bundle: parse ครั้งล่าสุดนานสุดกี่นาทีแล้ว
SELECT bundle_name, count(*) AS dags,
       round(extract(epoch FROM now() - min(last_parsed_time)) / 60) AS oldest_parse_min
FROM dag WHERE NOT is_stale GROUP BY 1 ORDER BY 1;
```

**ตรวจเพิ่ม:**
- Import Errors ใน UI ไม่มีรายการใหม่
- log ของ `airflow-dag-processor-pilot` ไม่มี error: `kubectl -n $NS logs deploy/airflow-dag-processor-pilot --since=15m | grep -i error`
- trigger DAG ของ APzzzz-WWWWW 1 ตัว แล้วดูว่า task รันผ่าน (worker หาไฟล์ใน bundle `pilot` เจอ)
- ลองแก้ `doc_md` ของ DAG ใน APzzzz-WWWWW แล้ว deploy จับเวลาจนมี version ใหม่ใน UI

**อ่านผล**

| ผล | แปลว่า |
|---|---|
| `pilot` มี `oldest_parse_min` ไม่กี่นาที (เดิม ~75–90) และ deploy ขึ้นภายในไม่กี่นาที | เวลาที่รอเกิดจาก**คิวรวม** การแยก bundle ได้ผล ไปทำส่วน B |
| `pilot` ยังนานใกล้เคียงเดิม | คอขวดอยู่ที่อื่น เช่น การ sync จาก blob หรือระบบภายนอกที่ DAG เรียก ให้ตรวจก่อนทำส่วน B |
| DAG ของ APzzzz-WWWWW เป็น stale หรือมี import error | ปัญหา path หรือ import โค้ดร่วม ดูส่วน B ขั้นตอนที่ 1 (`PYTHONPATH`) แล้ว rollback |

## A7. Rollback (ย้อนลำดับ)

```sh
# 1) ลบบรรทัด <company_name>/APzzzz-WWWWW/ ออกจาก .airflowignore บน blob
# 2) ลบ dag-processor ของ pilot
kubectl -n $NS delete deploy airflow-dag-processor-pilot
# 3) คืน args เดิมของ dag-processor
kubectl -n $NS patch deploy airflow-dag-processor --type json -p \
  "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":$(cat dag-processor-args-original.json)}]"
# 4) ลบ bundle config ออกจากทุก component (pod จะ restart)
kubectl -n $NS set env deploy,sts -l 'component in (scheduler,api-server,triggerer,worker,dag-processor)' \
  AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST-
```

ตรวจหลัง rollback: DAG ของ APzzzz-WWWWW กลับมาอยู่ `dags-folder` และไม่เป็น stale (ใช้ SQL ข้อ 1 ใน A6) แล้วค่อยเปิด GitOps sync กลับ

---

# ส่วน B: ทำจริง (Helm)

## ขั้นตอนที่ 0: ตรวจเวอร์ชันและ CLI

```sh
helm -n airflow list                                              # ชื่อ release และเวอร์ชันของ chart
helm -n airflow get values airflow -o yaml > values-current.yaml  # สำรอง values ปัจจุบัน
kubectl -n airflow exec deploy/airflow-dag-processor -- airflow version
kubectl -n airflow exec deploy/airflow-dag-processor -- airflow dag-processor --help | grep -i bundle
kubectl -n airflow exec deploy/airflow-dag-processor -- \
  airflow config get-value dag_processor dag_bundle_config_list
```

ผลที่ควรได้:
- `--help` มี option `--bundle-name`
- `dag_bundle_config_list` เป็น JSON list ที่มี bundle ชื่อ `dags-folder` และ path `/opt/airflow/dags` (หรือว่าง ซึ่งแปลว่าใช้ค่าเริ่มต้นแบบนี้)

ถ้าไม่ตรง **ให้หยุด** แล้วปรับคู่มือตามเวอร์ชันจริงก่อน

---

## ขั้นตอนที่ 1: ทดสอบว่า DAG parse ได้เมื่อ root เป็น folder ของ app

จุดที่พังบ่อยที่สุดคือ **import โค้ดร่วม** ที่อ้างจาก `/opt/airflow/dags` ทดสอบใน pod เดิมได้เลย ขั้นตอนนี้ไม่เปลี่ยนแปลงอะไรในระบบ

```sh
kubectl -n airflow exec -it deploy/airflow-dag-processor -- bash

# 1) ดู module ที่ import บ่อย เพื่อหาโค้ดร่วม
grep -rhoE '^\s*(from|import)\s+[A-Za-z_][A-Za-z0-9_.]*' \
  /opt/airflow/dags/<company_name>/APxxxx-YYYYY /opt/airflow/dags/<company_name>/APzzzz-WWWWW \
  | awk '{print $2}' | sort | uniq -c | sort -rn | head -20

# 2) parse เหมือน bundle ใหม่ แล้วนับ DAG และ import error
python - <<'PY'
try:
    from airflow.dag_processing.dagbag import DagBag   # Airflow 3.1+
except ImportError:
    from airflow.models.dagbag import DagBag
for path in ["/opt/airflow/dags/<company_name>/APxxxx-YYYYY", "/opt/airflow/dags/<company_name>/APzzzz-WWWWW"]:
    bag = DagBag(path, include_examples=False)
    print(path, "dags:", len(bag.dags), "import errors:", len(bag.import_errors))
    for f, e in list(bag.import_errors.items())[:3]:
        print("   ", f, e.strip().splitlines()[-1])
PY
```

ผลที่ควรได้: **APxxxx-YYYYY ประมาณ 426 DAG, APzzzz-WWWWW ประมาณ 30 DAG และไม่มี import error**

- ถ้ามี `ModuleNotFoundError` ให้ตั้ง `PYTHONPATH=/opt/airflow/dags` ในขั้นตอนที่ 2 แล้วทดสอบซ้ำด้วย `PYTHONPATH=/opt/airflow/dags python - <<'PY' ...`
- ขั้นตอนนี้ใช้เวลานาน เพราะต้อง parse APxxxx-YYYYY จริงทั้งหมด (ประมาณ 8 ชม. ของเวลา parse ถ้ารันแบบ process เดียว) ถ้าอยากเร็วขึ้น ให้ทดสอบเฉพาะไฟล์ตัวอย่างสัก 5–10 ไฟล์ก่อน

---

## ขั้นตอนที่ 2: เพิ่ม config ใน `values-bundles.yaml`

ตั้งผ่าน `config:` ของ chart เพื่อให้**ทุก component** (scheduler, API server, worker, triggerer, dag-processor) ได้ค่าเดียวกัน จำเป็นเพราะ worker ต้องหาไฟล์ DAG ผ่าน bundle ตอนรัน task

```yaml
config:
  dag_processor:
    dag_bundle_config_list: >-
      [
        {"name": "dags-folder",
         "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
         "kwargs": {"path": "/opt/airflow/dags"}},
        {"name": "heavy",
         "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
         "kwargs": {"path": "/opt/airflow/dags/<company_name>/APxxxx-YYYYY"}},
        {"name": "pilot",
         "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
         "kwargs": {"path": "/opt/airflow/dags/<company_name>/APzzzz-WWWWW"}}
      ]

# ใส่เฉพาะถ้าขั้นตอนที่ 1 พบ ModuleNotFoundError
env:
  - name: PYTHONPATH
    value: /opt/airflow/dags

# dag-processor เดิมของ chart ให้ดูแลเฉพาะ bundle เดิม
dagProcessor:
  args: ["bash", "-c", "exec airflow dag-processor --bundle-name dags-folder"]
  # ใส่เฉพาะถ้า A0 ยืนยันว่าเป็น H1 (2 pods parse ซ้ำ)
  # replicas: 1
```

ถ้าตั้ง `replicas: 1` ให้ตั้ง `AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES` ของ `dags-folder` ตาม CPU ที่ได้คืนมาด้วย แล้วเปลี่ยน `PARSING_PROCESSES` ตอนสร้างรายงานให้ตรงกับจำนวน process ที่ใช้จริง

ถ้า `values-current.yaml` มี `env:` อยู่แล้ว ให้**รวม** `PYTHONPATH` เข้าไปในรายการเดิม เพราะ Helm จะเขียนทับทั้ง list ไม่ได้ต่อท้าย

---

## ขั้นตอนที่ 3: สร้าง dag-processor deployment สำหรับ `heavy` และ `pilot`

chart ทางการมี dag-processor ได้แค่ deployment เดียว วิธีที่ปลอดภัยคือ **render ของเดิมจาก chart แล้วทำสำเนา** จะได้ volume, secret และ env ครบเหมือนเดิม

```sh
helm template airflow apache-airflow/airflow -n airflow --version <chart-version> \
  -f values-current.yaml -f values-bundles.yaml \
  --show-only templates/dag-processor/dag-processor-deployment.yaml > dag-processor-base.yaml

for b in heavy pilot; do
  procs=16; [ "$b" = pilot ] && procs=4
  yq "
    .metadata.name += \"-$b\" |
    .spec.replicas = 1 |
    .spec.selector.matchLabels.component = \"dag-processor-$b\" |
    .spec.template.metadata.labels.component = \"dag-processor-$b\" |
    (.spec.template.spec.containers[] | select(.name == \"dag-processor\")).args =
      [\"bash\", \"-c\", \"exec airflow dag-processor --bundle-name $b\"] |
    (.spec.template.spec.containers[] | select(.name == \"dag-processor\")).env +=
      [{\"name\": \"AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES\", \"value\": \"$procs\"}]
  " dag-processor-base.yaml > dag-processor-$b.yaml
done
```

**ข้อควรระวัง:**
- **ต้องเปลี่ยน label `component`:** ไม่อย่างนั้น selector ของ deployment เดิมจะนับ pod ใหม่เป็นของตัวเอง
- **path ของ template และชื่อ container** (`dag-processor`) ให้ตรวจจากผล `helm template` ของเวอร์ชันที่ใช้จริง
- **ถ้าจัดการ infra ด้วย Terraform หรือ GitOps** ให้เก็บ `dag-processor-heavy.yaml` และ `dag-processor-pilot.yaml` ไว้ใน repo เดียวกับ values แล้ว apply ผ่านช่องทางนั้น ไม่ใช้ `kubectl apply` ด้วยมือ
- **ทุกครั้งที่อัปเกรด chart** ต้อง render ไฟล์ทั้งสองใหม่ เพราะเป็นสำเนาจาก template ของเวอร์ชันเดิม

---

## ขั้นตอนที่ 4: `.airflowignore` ที่ root ของ blob

ใส่ไฟล์ `.airflowignore` ที่ root ของ container ใน blob storage (จะปรากฏเป็น `/opt/airflow/dags/.airflowignore`) เพื่อไม่ให้ bundle `dags-folder` parse APxxxx-YYYYY และ APzzzz-WWWWW ซ้ำ:

```
<company_name>/APxxxx-YYYYY/
<company_name>/APzzzz-WWWWW/
```

- ค่าเริ่มต้นเป็น regexp ถ้าตั้ง `[core] dag_ignore_file_syntax = glob` ต้องเขียนเป็น `<company_name>/APxxxx-YYYYY/**`
- ถ้ามี `.airflowignore` อยู่แล้ว ให้**เพิ่มบรรทัด** ไม่ใช่เขียนทับ
- ไฟล์นี้ไม่ส่งผลกับ bundle `heavy` และ `pilot` เพราะ bundle ทั้งสองเริ่มที่ folder ย่อย (ควรตรวจในขั้นตอนที่ 6)

---

## ขั้นตอนที่ 5: rollout (ทำในช่วงเวลาเดียวกัน)

> ⚠ **อย่าปล่อยให้ 2 bundle ดูแล DAG เดียวกันพร้อมกัน** ไม่อย่างนั้น `bundle_name` ของ DAG จะสลับไปมาตามว่า bundle ไหน parse ล่าสุด ให้ทำขั้นตอน 5.2–5.4 ต่อกันใน maintenance window เดียว

```sh
# 5.1 ดูการเปลี่ยนแปลงก่อน (ต้องมี plugin helm-diff)
helm diff upgrade airflow apache-airflow/airflow -n airflow --version <chart-version> \
  -f values-current.yaml -f values-bundles.yaml

# 5.2 อัปโหลด .airflowignore ขึ้น blob

# 5.3 อัปเกรด config ของทุก component
helm upgrade airflow apache-airflow/airflow -n airflow --version <chart-version> \
  -f values-current.yaml -f values-bundles.yaml

# 5.4 สร้าง dag-processor ใหม่
kubectl -n airflow apply -f dag-processor-heavy.yaml -f dag-processor-pilot.yaml
kubectl -n airflow rollout status deploy/airflow-dag-processor
kubectl -n airflow rollout status deploy/airflow-dag-processor-heavy
kubectl -n airflow rollout status deploy/airflow-dag-processor-pilot
```

---

## ขั้นตอนที่ 6: ตรวจหลัง rollout

```sql
-- DAG ที่ active ของแต่ละ bundle
-- ควรได้ประมาณ: dags-folder ~1,070 / heavy 426 / pilot 30
SELECT bundle_name, count(*) FROM dag WHERE NOT is_stale GROUP BY 1 ORDER BY 1;

-- ไม่ควรมี DAG ของ APxxxx-YYYYY/APzzzz-WWWWW กลายเป็น stale
SELECT dag_id, bundle_name, last_parsed_time FROM dag
WHERE is_stale
  AND (fileloc LIKE '%/APxxxx-YYYYY/%' OR fileloc LIKE '%/APzzzz-WWWWW/%');

-- DAG ของ APxxxx-YYYYY/APzzzz-WWWWW ต้องไม่อยู่ใน dags-folder แล้ว (ตรวจว่า .airflowignore ทำงาน)
SELECT bundle_name, count(*) FROM dag
WHERE NOT is_stale AND fileloc LIKE '%/APxxxx-YYYYY/%' GROUP BY 1;
```

**ตรวจเพิ่ม:**
- **Import Errors** ใน UI ต้องไม่มีรายการใหม่
- **log ของ dag-processor ทั้ง 3 ตัว** ต้องไม่มี error เรื่อง bundle:
  ```sh
  for d in airflow-dag-processor airflow-dag-processor-heavy airflow-dag-processor-pilot; do
    echo "== $d"; kubectl -n airflow logs deploy/$d --since=15m | grep -iE 'error|bundle' | tail -5
  done
  ```
- **trigger DAG ทดสอบ 1 ตัวจากแต่ละ bundle** เพื่อยืนยันว่า worker หาไฟล์เจอและรัน task ได้
- **วัดผล:** รัน export ซ้ำทุก 10–15 นาทีสัก 2–3 ชม. แล้วสร้างรายงานด้วย `./generate-report.sh` เทียบรอบก่อนและหลัง รายงานอ่านคอลัมน์ `bundle_name` อยู่แล้ว ให้ตั้ง `PARSING_PROCESSES` เท่ากับจำนวน process ที่**ทำงานได้จริง** (ถ้าเป็น H1 ให้นับแค่ pod เดียว) ไม่อย่างนั้นรอบที่คาดไว้จะเร็วกว่าจริง
- **ทดสอบกับ APzzzz-WWWWW:** deploy การแก้เล็ก ๆ ใน APzzzz-WWWWW แล้วจับเวลาจน version ใหม่ขึ้น UI เทียบกับก่อนแยก ถ้าลดจากระดับชั่วโมงเหลือไม่กี่นาที แปลว่าเวลารอเดิมเกิดจากคิวรวม

---

## Rollback

ทำย้อนลำดับ ภายใน maintenance window เดียวกัน:

```sh
kubectl -n airflow delete -f dag-processor-heavy.yaml -f dag-processor-pilot.yaml
# ลบ 2 บรรทัดที่เพิ่มออกจาก .airflowignore บน blob
helm upgrade airflow apache-airflow/airflow -n airflow --version <chart-version> -f values-current.yaml
```

หลังจากนั้น DAG ของ APxxxx-YYYYY และ APzzzz-WWWWW จะกลับมาอยู่ bundle `dags-folder` ในรอบ parse ถัดไป

---

## ทดสอบก่อนใช้กับเครื่องจริง

| ระดับ | สิ่งที่ทำ | กระทบระบบ? |
|---|---|---|
| 1 | ส่วน A ขั้นตอน A0 และส่วน B ขั้นตอนที่ 0–1 ใน pod เดิม | ไม่กระทบ |
| 1.5 | ส่วน A (A1–A7) ทดสอบ bundle จริงเฉพาะ APzzzz-WWWWW บน dev | pod ของ dev restart 2 ครั้ง (A2, A7) |
| 2 | `helm template`, `helm diff upgrade`, `helm upgrade --dry-run` | ไม่กระทบ |
| 3 | sandbox บน k3s ที่ เครื่องส่วนตัว (Airflow 3.2.1): สร้าง `<company_name>/APxxxx-YYYYY` และ `<company_name>/APzzzz-WWWWW` พร้อม DAG ตัวอย่าง 2–3 ไฟล์ แล้วทำขั้นตอน 2–6 ทั้งหมด | ไม่กระทบเครื่องของทีม |
| 4 | ทำทั้ง flow บน **dev** ก่อน แล้วค่อย **sit** | กระทบเฉพาะ env นั้น |

**สิ่งที่ต้องยืนยันใน sandbox (ระดับ 3):**
- `airflow dag-processor --bundle-name` ทำงาน และแต่ละ pod parse เฉพาะ bundle ของตัวเอง
- `.airflowignore` ที่ root ไม่ส่งผลกับ bundle ที่เริ่มที่ folder ย่อย
- ตอนย้าย DAG ข้าม bundle ประวัติ run และ version ยังอยู่ (เพราะ `dag_id` เดิม) และไม่มี DAG ค้างเป็น stale
- task ของ DAG ใน bundle ใหม่รันบน worker ได้

---

## ขั้นตอนถัดไป (ถ้าผลดี)

- เพิ่ม bundle ของ app อื่นตามเวลา parse (ดูส่วน "By application" ในรายงาน)
- แก้ต้นเหตุที่ DAG หลายร้อยตัวใช้ประมาณ 90 วินาทีต่อไฟล์ ได้ผลมากกว่าการแยก bundle (หาจุดที่ช้าด้วย `py-spy dump` หรือ `python -m cProfile -s cumtime <ไฟล์ DAG>` ใน pod ของ dag-processor)
- ให้ CI/CD เรียก API `PUT /api/v2/parseDagFile/{file_token}` หลัง deploy เพื่อให้ไฟล์ที่แก้ได้ parse ก่อนไฟล์อื่น
