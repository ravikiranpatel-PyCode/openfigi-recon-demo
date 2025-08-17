OpenFIGI Reconciliation Demo
Author: Ravi Patel
Date: August 2025

Overview
--------
This demo shows how Bloomberg’s OpenFIGI API can normalize fragmented security identifiers 
(ISINs, CUSIPs, SEDOLs, Tickers) across multiple counterparties into a single, consistent FIGI key.

Contents
--------
- recon_demo.py          : Python demo script
- fund_admin.csv         : Sample file (identifiers = ISINs)
- custodian.csv          : Sample file (identifiers = Tickers + CUSIPs)
- external_manager.csv   : Sample file (identifiers = CUSIPs)
- reconoutput_sample.xlsx: Example output (Before & After FIGI mapping)
- OpenFIGI-Final-RP.pptx : Presentation slides
- .env.example           : Template for API key

Requirements
------------
- Python 3.9+
- Packages: pandas, requests, openpyxl
  (install with: pip install pandas requests openpyxl)

Setup
-----
1. Copy `.env.example` to `.env`
2. Edit `.env` and add your OpenFIGI API key (get free key at https://www.openfigi.com/api#api-key)
   Example:
       OPENFIGI_API_KEY=your_api_key_here

Usage
-----
1. Run the script:
       python recon_demo.py

2. You will be prompted:
       Apply OpenFIGI mapping and Run Recon? (Y/N)

   - If you type "N": it saves only the raw 'Before' sheet.
   - If you type "Y": it calls the OpenFIGI API, maps IDs, and produces Before/After sheets.

3. Output:
   - An Excel file named `reconoutput_<timestamp>.xlsx`
   - Contains two sheets:
       a) Before_FIGI_Mapping – Raw identifiers from all three sources
       b) After_FIGI_Mapping  – FIGI-normalised reconciliation (summary + detail)

Notes
-----
- Script batches up to 100 identifiers per request to avoid rate limits.
- Raw request and response JSON for each record is saved in the output for transparency.
