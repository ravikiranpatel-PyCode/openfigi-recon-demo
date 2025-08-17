"""
3-Way Recon Demo using OpenFIGI
Author: Ravi Patel
Note: Quick demo for interview. Not a production-grade system. Do not share
An output file is produced with timestamp which shows custodian ticker format issue

"""

import pandas as pd
import requests, os, json, sys, time
from datetime import datetime

# -----------------------------------------------------------------------------
# settings
# -----------------------------------------------------------------------------
debugmode = 'n'     # set 'y' to see raw request/response in console
BATCH_SIZE = 100    # OpenFIGI limit
SLEEP_TIME = 0.25   # pause between batches

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outfile = f"reconoutput_{timestamp}.xlsx"

# -----------------------------------------------------------------------------
# load csv files
# -----------------------------------------------------------------------------
def load_file(fname, source):
    df = pd.read_csv(fname, dtype=str)
    df["Source"] = source
    return df[["SecurityID","IDType","Quantity","Price","SecurityName","Source"]]

fund_admin = load_file("fund_admin.csv","Fund Admin")
custodian  = load_file("custodian.csv","Custodian")
ext_mgr    = load_file("external_manager.csv","External Manager")

before_output = pd.concat([fund_admin,custodian,ext_mgr], ignore_index=True)

# -----------------------------------------------------------------------------
# ask user if they want to run mapping
# -----------------------------------------------------------------------------
choice = input("Apply OpenFIGI mapping and Run Recon? (Y/N, default=N): ").strip().lower()
if choice not in ['y','yes']:
    with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
        before_output.to_excel(writer, sheet_name="Before_FIGI_Mapping", index=False)
    print(f"Saved BEFORE mapping results only -> {outfile}")
    sys.exit(0)
else:
    print("Running full recon...\n")

# -----------------------------------------------------------------------------
# build mapping jobs
# -----------------------------------------------------------------------------
def make_job(idtype, val):
    job = {"idValue": str(val).strip()}
    t = str(idtype).upper().strip()
    if t == "ISIN":
        job["idType"] = "ID_ISIN"
    elif t == "CUSIP":
        job["idType"] = "ID_CUSIP"
    elif t == "SEDOL":
        job["idType"] = "ID_SEDOL"
    elif t == "TICKER":
        job["idType"] = "TICKER"
        job["exchCode"] = "US"   # assume US ticker for demo
    else:
        job["idType"] = "ID_BB_GLOBAL"
    return job

def build_jobs(df):
    return [make_job(row["IDType"], row["SecurityID"]) for _,row in df.iterrows()]

# -----------------------------------------------------------------------------
# call openfigi api in batches
# -----------------------------------------------------------------------------
def map_to_figi(df):
    jobs = build_jobs(df)

    url = "https://api.openfigi.com/v3/mapping"
    headers = {"Content-Type": "application/json"}
    key = os.getenv("OPENFIGI_API_KEY")
    if key:
        headers["X-OPENFIGI-APIKEY"] = key

    results, raw_resps = [], []

    for i in range(0, len(jobs), BATCH_SIZE):
        batch = jobs[i:i+BATCH_SIZE]
        try:
            r = requests.post(url, headers=headers, json=batch)
            if r.status_code != 200:
                print(f"API error {r.status_code}: {r.text}")
                results.extend([None]*len(batch))
                raw_resps.extend([{"error":r.text}]*len(batch))
                continue

            data = r.json()
            raw_resps.extend(data)
            for res in data:
                if "data" in res and res["data"]:
                    results.append(res["data"][0])
                    if debugmode=='y':
                        print("Mapped:", res["data"][0].get("figi"))
                else:
                    results.append(None)
                    if debugmode=='y':
                        print("No mapping found")
        except Exception as e:
            print("Request failed:", e)
            results.extend([None]*len(batch))
            raw_resps.extend([{"error":str(e)}]*len(batch))
        time.sleep(SLEEP_TIME)

    figi_cols = ["figi","compositeFIGI","securityType","securityType2","marketSector",
                 "exchangeCode","shareClassFIGI","currency","status","expiration",
                 "coupon","maturity","ticker","name"]

    expanded = []
    for idx,(res,job,raw) in enumerate(zip(results,jobs,raw_resps)):
        row = df.iloc[idx].to_dict()
        row["OpenFIGI_Request"] = json.dumps(job)
        row["OpenFIGI_Response"] = json.dumps(raw)
        if res:
            for c in figi_cols: row[c] = res.get(c)
        else:
            for c in figi_cols: row[c] = None
        expanded.append(row)

    return pd.DataFrame(expanded)

# -----------------------------------------------------------------------------
# apply mapping
# -----------------------------------------------------------------------------
print("Mapping Fund Admin...")
fund_admin = map_to_figi(fund_admin)
print("Mapping Custodian...")
custodian  = map_to_figi(custodian)
print("Mapping External Manager...")
ext_mgr    = map_to_figi(ext_mgr)

after_mapping = pd.concat([fund_admin,custodian,ext_mgr], ignore_index=True)

# -----------------------------------------------------------------------------
# build recon key
# -----------------------------------------------------------------------------
after_mapping["ReconKey"] = after_mapping.apply(
    lambda x: x["compositeFIGI"] if pd.notna(x["compositeFIGI"]) and str(x["compositeFIGI"]).strip() else x["figi"],
    axis=1
)

# -----------------------------------------------------------------------------
# recon process
# -----------------------------------------------------------------------------
rows = []
for key, grp in after_mapping.groupby("ReconKey", dropna=False):
    reconkey = key if pd.notna(key) and str(key).strip() else "NO_FIGI_MAPPING"
    qtys = grp.set_index("Source")["Quantity"]

    if len(set(qtys)) == 1:
        status = "Matched"
    else:
        diffs = []
        for i, p1 in enumerate(qtys.index):
            for p2 in list(qtys.index)[i+1:]:
                if qtys[p1] != qtys[p2]:
                    diffs.append(f"{p1}({qtys[p1]}) vs {p2}({qtys[p2]})")
        status = "; ".join(diffs) if diffs else "Mismatch"

    # summary row
    if reconkey != "NO_FIGI_MAPPING":
        first = grp.iloc[0]
        rows.append({
            "ReconKey": reconkey,
            "SecurityName": first["name"] or first["SecurityName"],
            "MarketSector": first["marketSector"],
            "Currency": first["currency"],
            "SecurityType": first["securityType"],
            "TotalQuantity": grp["Quantity"].astype(float).sum(),
            "MatchStatus": status,
            "Source": "SUMMARY"
        })

    # detail rows
    for _,r in grp.iterrows():
        rows.append({
            "ReconKey": "" if reconkey!="NO_FIGI_MAPPING" else "NO_FIGI_MAPPING",
            "SecurityName": r["name"] if reconkey!="NO_FIGI_MAPPING" else r["SecurityName"],
            "MarketSector": r["marketSector"] if reconkey!="NO_FIGI_MAPPING" else "",
            "Currency": r["currency"] if reconkey!="NO_FIGI_MAPPING" else "",
            "SecurityType": r["securityType"] if reconkey!="NO_FIGI_MAPPING" else "",
            "TotalQuantity": "",
            "MatchStatus": "",
            "Source": r["Source"],
            "SecurityID": r["SecurityID"],
            "IDType": r["IDType"],
            "Quantity": r["Quantity"],
            "Price": r["Price"],
            "FIGI": r["figi"] if reconkey!="NO_FIGI_MAPPING" else "",
            "exchangeCode": r["exchangeCode"],
            "shareClassFIGI": r["shareClassFIGI"],
            "status": r["status"],
            "OpenFIGI_Request": r["OpenFIGI_Request"],
            "OpenFIGI_Response": r["OpenFIGI_Response"]
        })

final_output = pd.DataFrame(rows)

# -----------------------------------------------------------------------------
# save to excel
# -----------------------------------------------------------------------------
with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    before_output.to_excel(writer, sheet_name="Before_FIGI_Mapping", index=False)
    final_output.rename(columns={"ReconKey":"CompositeFIGI/FIGI"}).to_excel(
        writer, sheet_name="After_FIGI_Mapping", index=False
    )

print(f"\nRecon complete. Results saved -> {outfile}")
