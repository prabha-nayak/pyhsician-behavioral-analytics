# Physician Behavioral Analytics - Medicare Provider Insights (2022)

### *What does physician behavior actually look like at scale and what can 9.7 million records tell us about how doctors practice medicine?*

**[📊 View Interactive Tableau Dashboard](https://public.tableau.com/app/profile/prabha.nayak3918/viz/PhysicianBehavioralAnalyticsMedicareProviderInsights2022/PhysicianBehavioralAnalyticsMedicareProviderInsights2022)** &nbsp;|&nbsp; **[Data Source: CMS Medicare 2022](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service)**

---

## The Question That Started This

Platforms like Doximity, Komodo Health, and similar health tech companies spend enormous resources trying to understand physician behavior: how doctors practice, what procedures they perform, how engaged they are with the healthcare system. But most of that analysis happens on proprietary data behind closed doors.

I wanted to answer a simpler version of that question using publicly available data: **Can we identify meaningful behavioral patterns across more than a million physicians using Medicare claims data alone?**

The answer, it turns out, is yes and the patterns are more striking than I expected.

---

## The Data

The Centers for Medicare & Medicaid Services publishes detailed provider-level data every year. The 2022 dataset contains every procedure billed to Medicare by every participating provider in the United States; **9,755,427 rows** covering **1,148,873 unique physicians** across all 50 states.

Each row tells you which physician billed for what procedure, how many times, how many patients were involved, what they charged and what Medicare actually paid. It's one of the most detailed windows into real-world physician practice patterns that exists in the public domain.

To keep the analysis focused and computationally manageable, I filtered to the **top 5 states by provider count**: California, New York, Texas, Florida, and Pennsylvania leaving a working dataset of **3.2 million records** representing **377,000 physicians**.

Processing 9.7 million rows from a 3GB file required moving beyond standard Pandas. This project was built using **PySpark 4.0 on Google Colab**, which handled the distributed processing and allowed me to cache filtered datasets in memory for fast downstream querying.

---

## What the Data Reveals

### 1. Volume and value are almost entirely disconnected

The specialty with the highest Medicare service volume, **Clinical Laboratory**, has one of the lowest average Medicare payments at just **$43 per service**. Meanwhile, **Hematology-Oncology** processes moderate volume but commands over **$270 per service** on average, driven almost entirely by high-cost cancer drug infusions.

This disconnect between how busy a physician is and how much Medicare pays them tells an important story: activity alone is a poor proxy for clinical or financial impact.

![Specialty Utilization Dashboard](#)

---

### 2. Medicare pays roughly 20 cents on the dollar

Across all providers and procedures, the average submitted charge was **$399**. The average Medicare payment was **$82**, a payment ratio of about **20.5%**.

This isn't a bug in the system, it's by design. Medicare negotiates rates far below what providers bill. But the variation across specialties is striking:

- **Pharmacy** has the highest payment ratio at **61.3%**. Medicare covers most drug costs
- **Ambulance Service** has the lowest at **21.1%**, transport is heavily discounted
- **Orthopedic Surgery** submits the highest average charges ($888) but only collects 17.8 cents per dollar

---

### 3. Geography matters but not in the way you'd expect

Pennsylvania has the **highest Medicare payment ratio at 23.3%**, meaning Medicare covers a greater share of submitted charges there than anywhere else in our top 5 states. Texas has the lowest at **18.5%**.

California has the largest total Medicare spend by a wide margin, driven by its sheer volume of providers. But Florida's providers bill more aggressively relative to their payment suggesting different regional pricing behaviors.

---

### 4. Most physicians are minimally engaged with Medicare

This was the most surprising finding. When I segmented all 377,000 providers into behavioral profiles based on service volume and average payment, the distribution was heavily skewed:

| Segment                 | Providers | Share | Avg Services | Avg Medicare Payment |
| ----------------------- | --------: | ----: | -----------: | -------------------: |
| Low Volume, Low Value   |   195,686 | 51.7% |          315 |                  $61 |
| High Volume, Low Value  |    93,028 | 24.6% |        7,509 |                  $56 |
| Low Volume, High Value  |    61,990 | 16.4% |          298 |                 $164 |
| High Volume, High Value |    26,662 |  7.0% |        7,181 |                 $198 |


More than half of all Medicare providers are low engagement on both dimensions. Only **7% of physicians** fall into the High Volume, High Value segment yet this group likely drives a disproportionate share of total Medicare spend and represents the most strategically important segment for any platform trying to reach influential physicians.

---

### 5. The top procedures tell a story about modern medicine

The highest-volume procedures aren't surgical breakthroughs or complex diagnostics. They're **office visits, contrast agents for imaging, and iron infusions**. The COVID-19 test provision code (K1034) is still the third most common procedure in 2022 data, a visible reminder of how deeply the pandemic reshaped clinical workflows.

Drug and biologic procedures (flagged as `is_drug = Y`) cluster at the high-payment end of the distribution: cancer drugs, biologics, and specialty infusions are the procedures where Medicare payment most closely approaches submitted charges.

---

## The Technical Architecture

```
CMS Provider Data (3GB CSV, 9,755,427 rows)
         ↓
Google Drive (cloud storage + streaming)
         ↓
Google Colab + PySpark 4.0 (distributed processing)
         ↓
DataFrame caching — 3.2M filtered records held in memory
         ↓
Aggregated exports via Pandas → 5 CSV files
         ↓
Tableau Public — 6-chart interactive dashboard
```

One lesson learned the hard way: calling `.count()` inside a loop to check null values triggered a full 9.7M-row scan for every single column — the job ran for 13 minutes before I caught it. Replacing it with a single-pass `F.when()` scan reduced that to under 2 minutes. Small decisions in Spark architecture matter enormously at scale.

---

## Dashboard

The final dashboard includes 6 visualizations built to tell a layered story:

1. **Specialty Utilization** - ranked bar chart showing volume across 20 specialties
2. **State Payment Efficiency** - bubble scatter plot encoding payment ratio, volume, and state simultaneously
3. **Top 25 Procedures** - packed bubble chart separating drug vs. non-drug procedures by color
4. **Provider Behavioral Segments** - four-color segmentation of 377K physicians
5. **Specialty × State Heatmap** - 15×5 matrix showing where payment rates diverge across geographies
6. **Volume vs. Payment Dual Axis** - bar and line overlay revealing where volume and value diverge

**[→ Explore the Dashboard](https://public.tableau.com/app/profile/prabha.nayak3918/viz/PhysicianBehavioralAnalyticsMedicareProviderInsights2022/PhysicianBehavioralAnalyticsMedicareProviderInsights2022)**

---

## Tools Used

| Layer                  | Tool               |
| ---------------------- | ------------------ |
| Distributed Processing | PySpark 4.0.2      |
| Cloud Runtime          | Google Colab       |
| Cloud Storage          | Google Drive       |
| Data Wrangling         | Python, Pandas     |
| Visualization          | Tableau Public     |
| Version Control        | Git                |
| AI Workflow Support    | Claude (Anthropic) |


---

## How to Reproduce

1. Download the [CMS Medicare Physician & Other Practitioners 2022](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service) dataset
2. Upload to a Google Drive folder
3. Open `01_spark_ingestion.ipynb` in Google Colab
4. Mount Drive, install PySpark, and run all cells in order
5. Connect Tableau to the exported CSVs in `/output`

---

## What I'd Explore Next

This analysis scratches the surface. With more time I'd want to:
- **Add year-over-year comparison** - 2019 vs 2022 to quantify COVID's lasting impact on physician practice patterns
- **Map procedure mix to physician specialization index** - are high-value physicians more or less specialized than their peers?
- **Layer in demographic data** - do physician behavioral segments correlate with patient demographics in their geography?

---

*Analysis by Prabha Nayak 
*Data: CMS Medicare Program, 2022 Reporting Year | Built with PySpark 4.0.2*
