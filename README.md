
---

## 🔷 Capas del Modelo Medallón

### 🟤 Bronze - Ingesta cruda en streaming
- Lee eventos JSON en tiempo casi real desde Azure Event Hub.
- Agrega metadatos como `ingestion_time` y `event_timestamp`.
- Escribe los datos crudos en formato **Delta Lake**, con checkpointing habilitado.
- Se almacena en la ruta: `/mnt/bronze/sales`

### ⚪ Silver - Limpieza y enriquecimiento
- Elimina duplicados, datos inválidos y normaliza campos.
- Genera un hash de la transacción (`transaction_hash`) y clasifica el ticket (`ticket_category`).
- Une con tablas batch externas:
  - `products.csv`, `stock.csv`, `stores.csv`, `channels.csv`, `customers.csv`.
- Produce dos salidas:
  - **Silver Simple**: transacciones limpias, sin enriquecimiento.
  - **Silver Enriched**: transacciones limpias + contexto enriquecido.
- Almacenamiento:
  - `/mnt/silver/sales/simple`
  - `/mnt/silver/sales/enriched`

### 🟡 Gold - KPIs de negocio
Agrupaciones y métricas construidas en batch a partir de la Silver enriquecida.

#### 1. `sales_by_day`
- Total de transacciones, monto total y ticket promedio por día, canal y región.
- `/mnt/gold/sales/sales_by_day`

#### 2. `top_customers`
- Clientes con mayor gasto acumulado, cantidad de compras y última compra.
- `/mnt/gold/sales/top_customers`

#### 3. `product_performance`
- Productos más vendidos, ingresos generados y ticket promedio por producto.
- `/mnt/gold/sales/product_performance`

---

## ⏱️ ¿Por qué este pipeline es "Casi Tiempo Real"?

Este pipeline utiliza **Spark Structured Streaming**, que opera en **micro-lotes** (por defecto cada 1 segundo), lo que permite procesar grandes volúmenes de datos casi en tiempo real.

### ¿Qué lo diferencia del "tiempo real"?

- En **tiempo real**, el sistema reacciona en milisegundos sin buffer ni almacenamiento intermedio (ej: airbag o detección de fraude).
- En **casi tiempo real** (como este caso), hay una **mínima latencia** debido al procesamiento por lotes pequeños, escritura en Delta Lake y uso de checkpoints.
- Este enfoque es **ideal para análisis, monitoreo y visualización** sin necesidad de respuestas inmediatas.

---

## 🚀 ¿Cómo ejecutar el proyecto?

1. **Configurar Azure:**
   - Crear un Event Hub y enviar eventos JSON simulados de ventas.
   - Configurar un Data Lake Storage Gen2 con containers: `rawmarket`, `bronze`, `silver`, `gold`.

2. **Cargar archivos batch en `/mnt/rawmarket/csv-raw/`:**
   - `products.csv`, `stock.csv`, `stores.csv`, `channels.csv`, `customers.csv`.

3. **Montar los containers en Databricks:**
   - Usando `dbutils.fs.mount` con OAuth 2.0.

4. **Ejecutar notebooks:**
   - `bronze_streaming_ingestion`: conecta con Event Hub y guarda en Delta.
   - `silver_enrichment`: limpia, transforma y une con los archivos batch.
   - `gold_kpis`: calcula KPIs por cliente, producto y día.

5. **Ver los resultados en Power BI o dashboards analíticos conectados a las tablas Delta.**

---

## 🧰 Tecnologías utilizadas

| Tecnología         | Rol                                  |
|-------------------|---------------------------------------|
| Azure Event Hub    | Ingesta de eventos en tiempo real     |
| Azure Data Lake Gen2 | Almacenamiento por capas              |
| Azure Databricks   | Procesamiento distribuido con Spark   |
| Delta Lake         | Formato transaccional con versionado  |
| Structured Streaming | Procesamiento casi en tiempo real     |
| PySpark            | Transformaciones y lógica de negocio  |
| Mount / Secrets    | Acceso seguro a Storage y Event Hub   |

---

## 📁 Estructura de carpetas

