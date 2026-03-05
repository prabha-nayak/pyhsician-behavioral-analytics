# Physician Behavioral Analytics — Medicare Provider Insights (2022)

## Project Overview
End-to-end analytics pipeline analyzing 9.7 million Medicare provider 
records to identify and classify behavioral patterns of medical 
professionals, directly mirroring the analytical work done at 
physician-focused health tech platforms.

**[View Interactive Tableau Dashboard] https://public.tableau.com/app/profile/prabha.nayak3918/viz/PhysicianBehavioralAnalyticsMedicareProviderInsights2022/PhysicianBehavioralAnalyticsMedicareProviderInsights2022**  
**[Data Source: CMS Medicare 2022](https://data.cms.gov)**

---

## Pipeline Architecture
```
Raw CMS Data (3GB CSV, 9.7M rows)
        ↓
Google Colab + PySpark 4.0 — Distributed Processing
        ↓
Python/Pandas — Feature Engineering & Segmentation
        ↓
CSV Exports — Tableau-Ready Datasets
        ↓
Tableau Public — 6-Chart Interactive Dashboard
```

---

## Key Findings
- **1,148,873 unique physicians** across the US : CA leads with 90,432 providers
- **Clinical Laboratory dominates volume** at 120M+ services but has one of the lowest Medicare payment rates at $43 avg
- **Hematology-Oncology has the highest payment spike** : moderate volume but $270+ avg Medicare payment driven by high-cost drug infusions
- **PA has the highest Medicare payment ratio at 23.3%** : most efficient state; TX has the lowest at 18.5%
- **Only 26,662 providers (6.9%) are High Volume High Value** : the most strategically important physician segment
- **195,686 providers (51.7%) are Low Volume Low Value** : the majority of Medicare providers have minimal engagement
- **Ambulance Service has the lowest payment ratio at 21.1%** despite high submitted charges : Medicare heavily discounts 
  transport costs

---

## Distributed Processing Highlights
- Processed **9.7M rows from a 3GB CSV** using PySpark 4.0 on Google Colab
- Used **DataFrame caching** to store 3.2M filtered records in memory — reducing subsequent query time from minutes to seconds
- Replaced per-column `.count()` loops with **single-pass null checking** using `F.when()` reducing scan time by ~90%
- Filtered to top 5 states using **Spark filter + cache pattern** for efficient downstream aggregations
- Exported aggregated results to **Pandas for CSV export** demonstrating Spark-to-Pandas workflow

---

## Tools & Technologies
| Layer                  | Tool              | Purpose                         |
| ---------------------- | ----------------- | ------------------------------- |
| Distributed Processing | PySpark 4.0       | Read and process 9.7M rows      |
| Cloud Runtime          | Google Colab      | Scalable compute environment    |
| Feature Engineering    | Python (Pandas)   | Segmentation, aggregation       |
| Visualization          | Tableau Public    | 6-chart interactive dashboard   |
| Data Source            | CMS Medicare 2022 | Provider-level utilization data |
| AI Tools               | Claude, ChatGPT   | Workflow acceleration           |

---

## Dashboard Visualizations
1. **Specialty Utilization** — horizontal bar chart of top 20 
   specialties by total Medicare services
2. **State Payment Efficiency** — scatter plot with sized bubbles 
   showing payment ratio vs submitted charges by state
3. **Top Procedures Bubble Chart** — packed bubble chart colored 
   by drug vs non-drug procedure type
4. **Provider Behavioral Segments** — colored bar chart of 4 
   physician engagement segments
5. **Specialty × State Heatmap** — color matrix showing payment 
   patterns across 15 specialties and 5 states
6. **Volume vs Payment Dual Axis** — bar + line combo chart 
   showing where volume and value diverge by specialty

---

## Provider Segmentation Methodology
Physicians segmented into 4 behavioral profiles using 
volume and payment thresholds:

| Segment                | Providers | Avg Services | Avg Payment |
| ---------------------- | --------- | ------------ | ----------- |
| Low Volume Low Value   |   195,686 |          315 |         $61 |
| High Volume Low Value  |    93,028 |        7,509 |         $56 |
| Low Volume High Value  |    61,990 |          298 |        $164 |
| High Volume High Value |    26,662 |        7,181 |        $198 |

Thresholds: Volume > 1,000 services | Payment > $100 avg

---

## Project Structure
```
physician-behavioral-analytics/
  notebooks/
    01_spark_ingestion.ipynb  ← PySpark ingestion, exploration,
                                 analysis, and export
  output/
    tableau_specialties.csv
    tableau_states.csv
    tableau_procedures.csv
    tableau_segments.csv
    tableau_heatmap.csv
```

---

## How to Reproduce
1. Download CMS Medicare Physician & Other Practitioners 2022 
   from data.cms.gov
2. Upload to Google Drive
3. Open notebook in Google Colab
4. Run all cells in order
5. Connect Tableau to output CSVs

---

*Analysis by Prabha Nayak | MS Health Informatics, 
University of San Francisco*  
*Data: CMS Medicare Program, 2022 Reporting Year*
