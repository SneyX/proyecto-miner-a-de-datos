# Proyecto: Cloud Provider Analytics
## ETL + Streaming + Serving en Cassandra
### Alumno: Tiago Prelato
### Fecha: 26/11/2025

---

## 📋 Descripción

Pipeline completo de datos para un **proveedor de nube** que implementa análisis de **FinOps, Soporte y Producto**. El sistema procesa datos de clientes para métricas operativas near real-time y batch diario/mensual.

```
Landing → Bronze → Silver → Gold → Serving (Cassandra/AstraDB)
```

Utilizando **PySpark** y siguiendo la **Arquitectura Lambda**, el pipeline:
- Procesa datos **batch** (maestros, facturación, tickets, NPS)
- Procesa datos **streaming** (eventos de uso near real-time)
- Aplica **reglas de calidad** y detección de **anomalías**
- Genera **5 marts de negocio** para dashboards
- Sirve datos desde **Cassandra/AstraDB**

---

## 🏗️ Arquitectura Lambda

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                          FUENTES DE DATOS (LANDING)                            ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║   BATCH (CSV)                          STREAMING (JSONL)                       ║
║   ├─ customers_orgs.csv                └─ usage_events_stream/*.jsonl         ║
║   ├─ users.csv                            • ~43K eventos                       ║
║   ├─ billing_monthly.csv                  • schema_version v1/v2              ║
║   ├─ support_tickets.csv                  • carbon_kg, genai_tokens (v2)      ║
║   ├─ resources.csv                                                             ║
║   └─ nps_surveys.csv                                                           ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                              BRONZE (Parquet)                                  ║
║   • Tipificación explícita (Schemas)                                          ║
║   • Columnas técnicas: ingest_ts, source_file                                 ║
║   • Structured Streaming con watermark y checkpointing                        ║
║   • Deduplicación por PK                                                       ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                              SILVER (Parquet)                                  ║
║   • 3 Reglas de calidad + Quarantine                                          ║
║   • 3 Métodos de detección de anomalías (Z-Score, MAD, Percentiles)          ║
║   • Manejo de schema_version v1/v2                                            ║
║   • Joins con maestros (customers, resources)                                 ║
║   • Features: daily_cost_usd, request_count, carbon_kg, genai_tokens         ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                              GOLD (5 Marts)                                    ║
║   FinOps:                                                                      ║
║   ├─ org_daily_usage_by_service (grano diario por org/servicio)              ║
║   ├─ revenue_by_org_month (revenue USD normalizado)                          ║
║   └─ cost_anomaly_mart (anomalías con score)                                 ║
║   Soporte:                                                                     ║
║   └─ tickets_by_org_date (tickets, SLA breach rate, CSAT)                    ║
║   Producto/GenAI:                                                              ║
║   └─ genai_tokens_by_org_date (tokens, costo estimado)                       ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                           SERVING (AstraDB/Cassandra)                          ║
║   • 5 Colecciones query-first (todos los marts)                              ║
║   • 5 Consultas requeridas implementadas                                      ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

---

## 📁 Estructura del Proyecto

```
Proyecto Mineria/
├── Dataset/
│   └── datalake/
│       ├── landing/           # Datos crudos (CSV, JSONL)
│       │   ├── customers_orgs.csv
│       │   ├── users.csv
│       │   ├── billing_monthly.csv
│       │   ├── support_tickets.csv
│       │   ├── resources.csv
│       │   ├── nps_surveys.csv
│       │   └── usage_events_stream/*.jsonl
│       ├── bronze/            # Datos tipificados (Parquet)
│       ├── silver/            # Datos limpios y enriquecidos
│       ├── gold/              # 5 Marts de negocio
│       ├── quarantine/        # Registros con problemas de calidad
│       └── checkpoints/       # Checkpoints de Structured Streaming
├── segundo_parcial_pipeline.ipynb  # Notebook principal
├── requirements.txt                # Dependencias
└── README.md                       # Este archivo
```

---

## 🚀 Quickstart

### 1. Configurar el entorno

```bash
# Crear virtual environment
python -m venv venv

# Activar (Windows)
.\venv\Scripts\activate

# Activar (Linux/Mac)
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Ejecutar el Pipeline

```bash
jupyter notebook segundo_parcial_pipeline.ipynb
```

### 3. Verificar resultados

El pipeline generará:
- Archivos Parquet en `bronze/`, `silver/`, `gold/`
- Checkpoints de streaming en `checkpoints/`
- Datos en AstraDB (5 colecciones)
- Registros de quarantine

---

## ✅ Checklist de Cumplimiento del Proyecto

### Requerimientos Técnicos Obligatorios

| Requisito | Estado | Evidencia |
|-----------|--------|-----------|
| **1. Ingesta** | | |
| Batch CSV a Bronze Parquet particionado | ✅ | 6 fuentes procesadas |
| Streaming: Structured Streaming | ✅ | `spark.readStream` |
| Schema explícito | ✅ | `events_schema` con StructType |
| withWatermark | ✅ | `withWatermark("timestamp", "1 hour")` |
| Dedup por event_id | ✅ | `dropDuplicates(["event_id"])` |
| Manejo late data | ✅ | Watermark de 1 hora |
| **2. Calidad de Datos** | | |
| Tipos consistentes | ✅ | Casting explícito en schemas |
| event_id no nulo y único | ✅ | Regla `event_id_not_null` |
| cost_usd_increment válido | ✅ | Regla `cost_valid` >= -0.01 |
| unit no nulo cuando value existe | ✅ | Regla `unit_when_value` |
| schema_version manejada | ✅ | Compatibilización v1/v2 |
| Quarantine en Parquet | ✅ | `quarantine/events/` |
| **3. Transformaciones Silver** | | |
| Normalización regiones/servicios | ✅ | `region_normalized` |
| Join a orgs/users/resources | ✅ | 2 joins implementados |
| Features calculadas | ✅ | daily_cost, requests, carbon, genai, cpu_hours |
| Flags de anomalía (3 métodos) | ✅ | Z-Score, MAD, Percentiles |
| **4. Gold Marts** | | |
| org_daily_usage_by_service | ✅ | FinOps - grano diario |
| revenue_by_org_month | ✅ | FinOps - revenue USD |
| cost_anomaly_mart | ✅ | FinOps - anomalías |
| tickets_by_org_date | ✅ | Soporte - SLA breach |
| genai_tokens_by_org_date | ✅ | GenAI - tokens/costo |
| **5. Serving AstraDB** | | |
| Keyspace/colecciones creadas | ✅ | 5 colecciones |
| Diseño query-first | ✅ | PKs optimizadas |
| Carga desde Spark | ✅ | Via astrapy batch |
| **6. Idempotencia** | | |
| Reprocesar sin duplicar | ✅ | Overwrite + cleanup |
| Checkpointing | ✅ | Habilitado |
| **7. Performance** | | |
| Particionado sensato | ✅ | Por event_date, month |
| **8. Documentación** | | |
| Diagrama arquitectura | ✅ | Incluido en README (sección Arquitectura Lambda) |
| Diccionario de datos | ✅ | En este README |
| Decisiones y trade-offs | ✅ | Log de decisiones |
| **9. Demo - 5 Consultas** | | |
| Q1: Costos/requests por org/servicio | ✅ | Con rango de fechas |
| Q2: Top-N servicios por costo | ✅ | Últimos 14 días |
| Q3: Tickets críticos + SLA breach | ✅ | Últimos 30 días |
| Q4: Revenue mensual USD | ✅ | Con créditos/impuestos |
| Q5: GenAI tokens y costo | ✅ | Por día |

---

## 📊 Reglas de Calidad de Datos

| Regla | Condición | Acción |
|-------|-----------|--------|
| `event_id_not_null` | event_id IS NOT NULL | Quarantine si falla |
| `cost_valid` | cost_usd_increment >= -0.01 | Quarantine si falla |
| `unit_when_value` | IF value NOT NULL THEN unit NOT NULL | Quarantine si falla |

---

## 🔍 Detección de Anomalías (3 Métodos)

| Método | Condición | Descripción |
|--------|-----------|-------------|
| **Z-Score** | \|z\| > 3 | Desviación > 3 std de la media |
| **MAD** | deviation > 3*median | Desviación absoluta de la mediana |
| **Percentiles** | > 1.5*p99 o < p01-0.1 | Fuera de rango intercuartil |

---

## 📈 Marts Gold

### 1. org_daily_usage_by_service (FinOps)
- **Grano**: Diario por organización y servicio
- **Métricas**: total_cost_usd, avg_cost, requests, carbon_kg, genai_tokens, anomaly_count

### 2. revenue_by_org_month (FinOps)
- **Grano**: Mensual por organización
- **Métricas**: subtotal_usd, credits_usd, taxes_usd, revenue_usd

### 3. tickets_by_org_date (Soporte)
- **Grano**: Diario por organización y severidad
- **Métricas**: ticket_count, sla_breach_rate, avg_csat, resolution_rate

### 4. genai_tokens_by_org_date (Producto)
- **Grano**: Diario por organización
- **Métricas**: total_tokens, total_cost_usd, estimated_cost_per_1k_tokens

### 5. cost_anomaly_mart (FinOps)
- **Grano**: Diario por organización y servicio
- **Métricas**: anomaly_count, avg_zscore, anomaly_severity

---

## 📝 Log de Decisiones Arquitectónicas

### Patrón: Arquitectura Lambda
**Justificación**: 
- **Batch**: Maestros (orgs, users, resources), facturación y encuestas NPS requieren procesamiento completo diario/mensual
- **Streaming**: Eventos de uso requieren métricas near real-time para dashboards operativos
- **Unificación**: Ambos flujos alimentan los mismos marts Gold

### Structured Streaming
- **Watermark**: 1 hora para tolerar late data sin bloquear el pipeline
- **Trigger**: `availableNow=True` para procesar todo lo disponible y terminar
- **Checkpointing**: Habilitado para exactly-once semantics y recovery

### Particionado
| Tabla | Partición | Justificación |
|-------|-----------|---------------|
| Events (Bronze/Silver/Gold) | `event_date` | Consultas por rango de fechas |
| Billing | `month` | Alineado con ciclo de facturación |
| Tickets | `created_at` | Consultas por fecha de creación |

### Claves Cassandra (AstraDB)
- **Partition Key**: `org_id` (distribución uniforme)
- **Clustering**: `event_date DESC` (consultas recientes primero)

### Umbrales de Calidad
| Umbral | Valor | Justificación |
|--------|-------|---------------|
| cost_valid | >= -0.01 USD | Permite pequeñas correcciones |
| Z-Score | > 3 | Estándar estadístico |
| Percentile | 1.5*p99 | Outliers extremos |

---

## 🔗 Credenciales AstraDB

```
ASTRA_DB_API_ENDPOINT=https://a0066dbd-d785-4122-8aa0-4b51ce1c06f1-us-east-2.apps.astra.datastax.com
ASTRA_DB_APPLICATION_TOKEN=AstraCS:hNlQRZNfohwaKPugMJwZNbCq:e8e5bdf53bbafd1fcf8f4f40682c92933b7478631c978b6fd8c64a007d777c1d
```

---

## 👤 Autor

**Tiago Prelato**  
Minería de Datos II  
Noviembre 2025
