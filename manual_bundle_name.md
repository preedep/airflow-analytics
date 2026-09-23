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

\* รอบที่คาดไว้ = เวลา parse รวม ÷ parsing_processes สมมติว่าแต่ละ process ไม่ได้แย่งระบบภายนอกตัวเดียวกัน ตอนนี้ (ถ้า 2 pods parse ซ้ำกัน) รอบที่วัดได้อยู่ที่ประมาณ 60–90 นาที

- **คงชื่อ `dags-folder` ไว้:** DAG ส่วนใหญ่จะไม่ต้องย้าย bundle มีแค่ APxxxx-YYYYY (426) กับ APzzzz-WWWWW (30) ที่ย้าย
- **APzzzz-WWWWW ใช้เป็นกลุ่มทดลอง:** median parse 88 วินาที มี 30 DAG เล็กพอจะเห็นผลชัดว่าเวลารอเกิดจากคิวรวมหรือจาก app เอง

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

# (แนะนำ) 2 pods ตอนนี้ parse ไฟล์ซ้ำกันหรือไม่: ถ้าไฟล์เดียวกันขึ้นใน log ทั้ง 2 pods แปลว่าซ้ำ
FILE=dev_<company_name>_apzzzz_wwwww_workflow_group2_corp.py   # ไฟล์ DAG ใดก็ได้ใน APzzzz-WWWWW
for p in $(kubectl -n $NS get pods -l component=dag-processor -o name); do
  echo "== $p"; kubectl -n $NS logs "$p" --since=3h | grep -F "$FILE" | tail -3
done
```

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
| `pilot` มี `oldest_parse_min` ไม่กี่นาที (เดิม ~60–90) และ deploy ขึ้นภายในไม่กี่นาที | เวลาที่รอเกิดจาก**คิวรวม** การแยก bundle ได้ผล ไปทำส่วน B |
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
```

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
- **วัดผล:** รัน export ซ้ำทุก 10–15 นาทีสัก 2–3 ชม. แล้วสร้างรายงานด้วย `./generate-report.sh` เทียบรอบก่อนและหลัง รายงานอ่านคอลัมน์ `bundle_name` อยู่แล้ว
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
