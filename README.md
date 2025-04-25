# 🧪 Datalake NBA DMC – AWS Glue ETL Jobs

Este repositorio contiene los scripts ETL desarrollados en **AWS Glue** para la gestión de datos del proyecto `datalake-nba-dmc`. Cada carpeta representa un Job independiente, automatizado y versionado a través de la integración directa con **GitHub** desde Glue Studio.

---

## 📂 Estructura del Repositorio

| Carpeta | Descripción |
|--------|-------------|
| `aws-job-landing-to-bronze` | ETL que transforma datos crudos desde la capa *landing* hacia *bronze*. |
| `aws-job-bronze-to-silver` | Limpieza, tipificación y normalización de datos desde *bronze* hacia *silver*. |
| `aws-job-silver-to-silver` | Enriquecimiento adicional o segmentación interna dentro de la capa *silver*. |
| `aws-job-silver-to-gold-player` | Carga transformada y modelada de datos a la capa *gold* con foco en jugadores. |
| `aws-job-silver-to-gold-team` | Carga transformada y modelada a la capa *gold* con enfoque en equipos. |

---

## ⚙️ Características Técnicas

- 📌 Glue Version: **4.0**
- 💡 Lenguaje: **Python 3 (Glue ETL Script Mode)**
- 🧠 Arquitectura: **Medallion (Landing → Bronze → Silver → Gold)**
- 🪣 Origen/Destino: **Amazon S3 (`datalake-nba-dmc`)**
- 🧪 Motores de transformación: **Spark (GlueContext + PySpark + DynamicFrames)**

---

## 🚀 Integración continua

Este repositorio está vinculado directamente con **AWS Glue Studio**, lo que permite:
- Versionar cambios automáticamente (`push from Glue`)
- Recuperar y sincronizar scripts directamente (`pull from GitHub`)
- Mantener trazabilidad clara del pipeline de datos

---

## 📌 Requisitos

- Permisos válidos de AWS IAM para ejecutar Glue Jobs
- Acceso al bucket `s3://datalake-nba-dmc/`
- Token GitHub con permisos `repo` (para sincronización)

---

## 🧼 Mantenimiento

Este repositorio es mantenido y versionado principalmente desde GitHub.  
Cada modificación o mejora en los scripts ETL se desarrolla en el repositorio, y luego es sincronizada en AWS Glue Studio mediante la opción `Pull from repository`.
---

