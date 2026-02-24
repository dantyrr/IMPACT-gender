# IMPACT Methodology

## Overview

IMPACT computes a **rolling 24-month impact factor** for scientific journals using freely available citation data from PubMed and NIH iCite. This document explains the methodology in detail and compares it to the official Clarivate Journal Impact Factor (JIF).

---

## Official Journal Impact Factor (JIF)

Clarivate defines the JIF for year Y as:

```
JIF(Y) = Citations in year Y to articles published in years Y-1 and Y-2
         ÷ Number of citable articles published in years Y-1 and Y-2
```

Key characteristics:
- **Annual** — computed once per year
- **Calendar-year based** — ignores publication month within the year
- **"Citable items"** — typically research articles and reviews; editorials, letters, and corrections are excluded from the denominator but their received citations count in the numerator
- **Web of Science data** — proprietary, not publicly accessible

---

## IMPACT Rolling IF

IMPACT computes a **monthly rolling IF** using the same conceptual framework but with a sliding window:

```
Rolling IF at month M =

    Citations received in the 12-month window ending at month M
    by papers published in the 24-month window before that 12-month window

    ÷ Number of research articles in that 24-month publication window
```

### Windows Defined

For a target month `YYYY-MM`:

| Window | Period |
|--------|--------|
| Citation counting window | 12 months ending at YYYY-MM |
| Paper publication window | 24 months before the citation window starts |

**Example:** For target month 2025-01:
- Citation window: 2024-02 → 2025-01
- Paper window: 2022-02 → 2024-01

### Why Rolling Monthly?

The official JIF is released once per year in June, covering data from two calendar years prior. This means a journal's 2024 JIF is published in June 2025. IMPACT's rolling monthly approach provides:

1. **More frequent updates** — see IF trends as they happen
2. **Smoother signal** — less susceptible to a single year's outliers
3. **Earlier detection** — identify IF changes months before the official release

---

## Review-Excluded IF

IMPACT also computes a **review-excluded IF** where:
- Only research articles (not reviews) are in the **denominator**
- Only citations to research articles count in the **numerator**

This helps detect **review inflation**: some journals publish many highly-cited review articles specifically to boost their JIF. The gap between the standard rolling IF and the review-excluded IF indicates how much of the IF is driven by review articles.

```
Review-excluded IF = Citations to research articles in counting window
                    ÷ Research articles in publication window
```

---

## Data Sources

### PubMed E-utilities

Papers are discovered via the NCBI E-utilities API:

- **ESearch** — finds all PMIDs in a journal (by ISSN) within a year range
- **ESummary** — fetches metadata: title, publication date, publication type

Publication types used to classify reviews:
- `Review`, `Systematic Review`, `Meta-Analysis`, `Practice Guideline`, `Guideline`, `Consensus Development Conference` → classified as **Review**
- `Journal Article` → classified as **Research Article**
- `Editorial`, `Letter`, `Comment` → excluded from IF denominator

### NIH iCite API

Citation data is fetched from NIH iCite:

- Each paper record includes a `cited_by` field: a list of PMIDs of papers that cite it
- For each citing paper, its publication date (year/month) is fetched
- This allows reconstruction of **historical monthly citation counts**

### Historical Reconstruction

Because iCite provides the complete `cited_by` list with each citing paper's publication date, we can reconstruct the citation history retroactively:

```
For paper P published in 2022-03:
  iCite says: cited_by = [A, B, C, D, ...]
  We fetch pub dates: A (2022-06), B (2022-09), C (2023-01), ...
  → We know P received 1 citation in Jun 2022, 1 in Sep 2022, etc.
```

This enables computing the rolling IF for any past month, not just the present.

---

## Differences from Official JIF

| Aspect | Official JIF | IMPACT Rolling IF |
|--------|-------------|-------------------|
| Data source | Web of Science (proprietary) | PubMed + iCite (free) |
| Update frequency | Annual (June) | Monthly |
| Citation window | Calendar year | Rolling 12 months |
| Publication window | Calendar years Y-1 and Y-2 | Rolling 24 months |
| Review handling | Reviews included in denominator | Reviews flagged, optional exclusion |
| Transparency | Black box | Fully open source |
| Journal coverage | ~21,000 journals | Any journal indexed by PubMed |

---

## Limitations

1. **PubMed ≠ Web of Science** — PubMed indexes a subset of journals (primarily biomedical/life sciences). WoS has broader coverage in some fields (engineering, social sciences).

2. **iCite month data** — iCite reliably provides publication year but month data may be approximate. Where month is unavailable, we default to month 6 (midpoint of year).

3. **Self-citations** — IMPACT does not currently exclude self-citations. Official JIF also includes self-citations by default (though Clarivate now reports a separate self-citation-excluded metric).

4. **Epub vs print dates** — We use the earliest available date (epub date when available via PubMed). The official JIF uses cover date.

5. **Citable items definition** — Clarivate's definition of "citable items" uses their editorial classification, which can differ from PubMed publication types.

---

## Interpreting the Results

- **Rolling IF > Official JIF**: IMPACT is capturing more citations, or the journal's citation rate is rising
- **Rolling IF < Official JIF**: Data coverage is partial, or IF is declining
- **Large gap between Rolling IF and Review-excluded IF**: The journal's IF is substantially driven by reviews — consider this when evaluating journal prestige
