# Databricks notebook source
# DBTITLE 1,Connection to ADLS
# ============================================================
# CONNECT AZURE DATABRICKS TO ADLS GEN2 (USING STORAGE KEY)
# ============================================================

# Replace with your real values
account_name = "stgsyntheticdata"        # storage account name
container_name = "raw"                   # your ADLS container name
account_key = "qbSJum0asirUIMUzS1AZgyCDwEVqGiU8DJSik7YR3bOfDxOvX+BlMFdL4BvQ+ASt6IyXlw=="
 # from Azure Portal -> Access keys

# Configure Spark for ADLS access
spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)

# Helper function for easy ABFSS path building
def abfss(path):
    return f"abfss://{container_name}@{account_name}.dfs.core.windows.net{path}"

print("Connected to ADLS Gen2 successfully!")
print("Example path:", abfss("/"))

# COMMAND ----------

# DBTITLE 1,12 Months transactions (Delta tables)
# ============================================================
#  SYNTHETIC RAW TRANSACTION GENERATOR 
# ============================================================

from datetime import datetime, timedelta
import calendar, random, uuid

from pyspark.sql import types as T
from pyspark.sql import Row

# ------------------------------------------------------------
# 1. BASIC CONFIG
# ------------------------------------------------------------

TARGET_YEAR = 2024

MIN_TXNS = 25000
MAX_TXNS = 30000

FRAUD_RATE_MIN = 0.10
FRAUD_RATE_MAX = 0.20

ATM_DENOMS = [10, 20, 30, 40, 50, 60, 80, 100, 150, 200, 250, 300]

HOME_CITY_SPEND_RATIO = 0.70
OTHER_UK_SPEND_RATIO  = 0.20
FOREIGN_SPEND_RATIO   = 0.10

# ------------------------------------------------------------
# 2. CURRENCY LOGIC
# ------------------------------------------------------------

euro_countries = [
    "France","Germany","Spain","Italy","Netherlands","Belgium",
    "Portugal","Ireland","Austria","Finland","Greece", "Sweden", "Norway", "Denmark", "Iceland", "Malta", 
    "Cyprus", "Luxembourg", "Slovenia", "Slovakia", "Czech Republic", "Poland", "Latvia", "Estonia", "Latvia",
     "Bulgaria", "Romania", "Hungary", "Croatia"
]

foreign_countries = euro_countries + ["USA"]

def get_currency(country: str) -> str:
    if country == "UK":
        return "GBP"
    if country == "USA":
        return "USD"
    if country in euro_countries:
        return "EUR"
    return "EUR"

# ------------------------------------------------------------
# 3. ACCOUNT POOL / HOME CITY
# ------------------------------------------------------------

accounts_df = spark.table("account_pool_master")
accounts_pdf = accounts_df.toPandas()

if "Usage_Weight" not in accounts_pdf.columns:
    accounts_pdf["Usage_Weight"] = 1.0

accounts_pdf["p"] = accounts_pdf["Usage_Weight"] / accounts_pdf["Usage_Weight"].sum()

uk_cities = [
    "London","Manchester","Birmingham","Glasgow","Liverpool","Leeds",
    "Bristol","Cardiff","Edinburgh","Belfast","York","Southampton",
    "Luton","Bournemouth","Newcastle","Nottingham"
]

if "Home_City" not in accounts_pdf.columns:
    import random as _r
    accounts_pdf["Home_City"] = [_r.choice(uk_cities) for _ in range(len(accounts_pdf))]

account_home_city_map = dict(zip(accounts_pdf["Account_Number"], accounts_pdf["Home_City"]))
external_home_city_map = {}

def choose_internal_row():
    return accounts_pdf.sample(weights=accounts_pdf["p"], n=1).iloc[0]

def choose_or_create_account():
    # 60% internal, 40% external
    if random.random() > 0.40:
        row = choose_internal_row()
        acc = int(row["Account_Number"])
        opening = float(row["Current_Balance"])
        return acc, opening, False
    else:
        acc = random.randint(90000000, 99999999)
        if acc not in external_home_city_map:
            external_home_city_map[acc] = random.choice(uk_cities)
        opening = float(random.uniform(0, 5000))
        return acc, opening, True

def home_city(acc: int) -> str:
    if acc in account_home_city_map:
        return account_home_city_map[acc]
    if acc in external_home_city_map:
        return external_home_city_map[acc]
    city = random.choice(uk_cities)
    external_home_city_map[acc] = city
    return city

# ------------------------------------------------------------
# 4. MERCHANT MODEL
# ------------------------------------------------------------

def build_merchant(min_amt, max_amt, category, channel="POS"):
    return {
        "min": min_amt,
        "max": max_amt,
        "category": category,
        "channel": channel,
        "mode": random.choice(["Chip","Contactless","Online","Mobile","Cash"])
    }


UK_MERCHANTS = {
    "Tesco":      {"min": 10, "max": 120, "category": "Groceries",  "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Asda":       {"min": 10, "max": 120, "category": "Groceries",  "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Sainsburys":  {"min": 10, "max": 130, "category": "Groceries",  "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "McDonalds":  {"min": 3,  "max": 25,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Costa":      {"min": 3,  "max": 25,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "KFC":        {"min": 4,  "max": 30,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Greggs":     {"min": 2,  "max": 20,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Shell":      {"min": 20, "max": 150, "category": "Fuel",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "BP":         {"min": 20, "max": 150, "category": "Fuel",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Boots":      {"min": 5,  "max": 80,  "category": "Pharmacy",  "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
}

GLOBAL_MERCHANTS = {
    "Amazon":      {"min": 10, "max": 1500, "category": "Shopping",      "channel": "Web",    "mode": "Online"},
    "Spotify":     {"min": 5,  "max": 30,   "category": "Entertainment", "channel": "Web",    "mode": "Online"},
    "Netflix":     {"min": 5,  "max": 25,   "category": "Entertainment", "channel": "Web",    "mode": "Online"},
    "Uber":        {"min": 5,  "max": 100,  "category": "Travel",        "channel": "Mobile", "mode": "Online"},
    "Google Play": {"min": 1,  "max": 100,  "category": "Digital",       "channel": "Web",    "mode": "Online"},
    "Apple Store": {"min": 15, "max": 2000, "category": "Electronics",   "channel": "Web",    "mode": "Online"},
    "McDonalds":  {"min": 3,  "max": 25,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "KFC":        {"min": 4,  "max": 30,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Dominos":    {"min": 5,  "max": 100,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Starbucks":  {"min": 5,  "max": 100,  "category": "Food",      "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
}

INTL_MERCHANTS = {
    "International Cafe":      {"min": 5,  "max": 250,   "category": "Food", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Global Electronics":      {"min": 50, "max": 2500, "category": "Electronics","channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Travel Center":           {"min": 20, "max": 300,  "category": "Travel", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Boutique Hall":           {"min": 30, "max": 400,  "category": "Clothing", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "International Pharmacy":  {"min": 5,  "max": 80,   "category": "Pharmacy","channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Raddison Hotel":         {"min": 20, "max": 1000,  "category": "Travel", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Burger King":            {"min": 3,  "max": 125,  "category": "Food", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Pizza Hut":              {"min": 3,  "max": 125,  "category": "Food","channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Subway":                 {"min": 3,  "max": 125,  "category": "Food ", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "Starbucks":              {"min": 5,  "max": 100,  "category": "Food", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
    "McDonalds":              {"min": 3,  "max": 125,  "category": "Food", "channel": "POS", "mode": random.choice(["Chip","Contactless"])},
}


def pick_merchant(pool: dict):
    name = random.choice(list(pool.keys()))
    return name, pool[name]

# ------------------------------------------------------------
# 5. TIMESTAMP HELPERS
# ------------------------------------------------------------

def random_timestamp(year, month):
    last = calendar.monthrange(year, month)[1]
    day = random.randint(1, last)
    hour = random.randint(7, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(year, month, day, hour, minute, second)

def night_timestamp(year, month):
    last = calendar.monthrange(year, month)[1]
    day = random.randint(1, last)
    hour = random.randint(0, 3)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(year, month, day, hour, minute, second)

# ------------------------------------------------------------
# 6. ROW CONSTRUCTOR & SCHEMA
# ------------------------------------------------------------

schema = T.StructType([
    T.StructField("Transaction_ID",   T.StringType(), True),
    T.StructField("Account_Number",   T.LongType(),   True),
    T.StructField("Timestamp",        T.StringType(), True),
    T.StructField("Type",             T.StringType(), True),
    T.StructField("Opening_Balance",  T.DoubleType(), True),
    T.StructField("Amount",           T.DoubleType(), True),
    T.StructField("Closing_Balance",  T.DoubleType(), True),
    T.StructField("Status",           T.StringType(), True),
    T.StructField("Transaction_Mode", T.StringType(), True),
    T.StructField("Category",         T.StringType(), True),
    T.StructField("Channel",          T.StringType(), True),
    T.StructField("Merchant_Name",    T.StringType(), True),
    T.StructField("Location",         T.StringType(), True),
    T.StructField("Country",          T.StringType(), True),
    T.StructField("Currency",         T.StringType(), True),
])

def make_row(acc, ts, ttype, opening, amount, closing, status,
             category, channel, mode, merchant, location, country):
    return (
        uuid.uuid4().hex[:12].upper(),
        int(acc),
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        ttype,
        float(opening),
        float(amount),
        float(closing),
        status.lower(),
        mode,
        category,
        channel.lower(),
        merchant,
        location,
        country.lower(),
        get_currency(country),
    )

# ------------------------------------------------------------
# 7. BASE TRANSACTION GENERATION
# ------------------------------------------------------------

def generate_base_transaction(acc, opening, timestamp, country, location):
    ttype = random.choice(["Withdrawal","Payment","Transfer"])
    status = "Completed"

    if ttype == "Withdrawal":
        amount = float(random.choice(ATM_DENOMS))
        merchant = random.choice(["HSBC ATM","Lloyds ATM","Barclays ATM","NatWest ATM"])
        category = "Cash Withdrawal"
        channel = "ATM"
        mode = "Cash"
        if amount > opening:
            status = "Declined"

    elif ttype == "Payment":
        pool = UK_MERCHANTS if country == "UK" else {**GLOBAL_MERCHANTS, **INTL_MERCHANTS}
        merchant, rule = pick_merchant(pool)
        amount = round(random.uniform(rule["min"], rule["max"]), 2)
        category = rule["category"]
        channel = rule["channel"]
        mode = rule["mode"]
        if amount > opening and country == "UK":
            status = "Declined"

    else:  # Transfer
        amount = round(random.uniform(50, 2000), 2)
        merchant = "Bank Transfer"
        category = "Bills"
        channel = "Web"
        mode = "Online"
        if amount > opening:
            status = "Declined"

    closing = opening
    if status == "Completed" and amount <= opening:
        closing -= amount

    row = make_row(
        acc, timestamp, ttype, opening, amount, closing, status,
        category, channel, mode, merchant, location, country
    )
    return row, closing

# ------------------------------------------------------------
# 8. FRAUD-LIKE PATTERNS (NO LABEL COLUMNS)
# ------------------------------------------------------------

def inject_repeated_declines(state, acc, month):
    """
    3–5 UK POS declines at local UK merchant.
    Uses UK_MERCHANTS pool instead of hardcoded Tesco.
    """
    rows = []
    base_ts = random_timestamp(TARGET_YEAR, month)
    opening = state[acc]["balance"]
    count = random.randint(3, 5)

    # pick one UK merchant for the whole streak (looks more realistic)
    merchant, rule = pick_merchant(UK_MERCHANTS)
    loc = home_city(acc)

    for i in range(count):
        ts = base_ts + timedelta(minutes=i)
        # force decline: amount above balance
        amount = opening + random.uniform(5, 60)

        rows.append(make_row(
            acc, ts, "Payment",
            opening, amount, opening, "Declined",
            rule["category"], rule["channel"], rule["mode"],
            merchant, loc, "UK"
        ))
        # balance does not change on decline

    return rows


def inject_back_to_back_retry(state, acc, month):
    """
    One declined UK POS attempt, then an immediate retry (same merchant type).
    Uses UK_MERCHANTS.
    """
    rows = []
    opening = state[acc]["balance"]
    ts = random_timestamp(TARGET_YEAR, month)

    # pick merchant for this pattern
    merchant, rule = pick_merchant(UK_MERCHANTS)
    loc = home_city(acc)

    # declined attempt (amount > opening)
    decline_amt = opening + random.uniform(5, 80)

    rows.append(make_row(
        acc, ts, "Payment",
        opening, decline_amt, opening, "Declined",
        rule["category"], rule["channel"], rule["mode"],
        merchant, loc, "UK"
    ))

    # retry with smaller amount
    success_amt = round(
        random.uniform(5, min(80, opening)) if opening > 0 else 5,
        2
    )
    if success_amt <= opening:
        status2 = "Completed"
        new_bal = opening - success_amt
    else:
        status2 = "Declined"
        new_bal = opening

    rows.append(make_row(
        acc, ts + timedelta(minutes=2), "Payment",
        opening, success_amt, new_bal, status2,
        rule["category"], rule["channel"], rule["mode"],
        merchant, loc, "UK"
    ))

    state[acc]["balance"] = new_bal
    return rows


def inject_high_velocity_small(state, acc, month):
    """
    4–7 small rapid digital payments (Spotify/Uber/Google Play).
    Uses GLOBAL_MERCHANTS digital subset.
    """
    rows = []
    base_ts = random_timestamp(TARGET_YEAR, month)
    opening = state[acc]["balance"]
    count = random.randint(4, 7)

    digital_pool = {
        "Spotify": GLOBAL_MERCHANTS["Spotify"],
        "Uber":    GLOBAL_MERCHANTS["Uber"],
        "Google Play": GLOBAL_MERCHANTS["Google Play"],
    }

    for i in range(count):
        merchant, rule = pick_merchant(digital_pool)
        amt = round(random.uniform(1, 15), 2)
        ts = base_ts + timedelta(
            seconds=random.randint(i * 15, (i + 1) * 30)
        )

        if amt <= opening:
            status = "Completed"
            closing = opening - amt
        else:
            status = "Declined"
            closing = opening

        rows.append(make_row(
            acc, ts, "Payment",
            opening, amt, closing, status,
            rule["category"], rule["channel"], rule["mode"],
            merchant, None, "UK"   # online, no physical location
        ))

        opening = closing
    state[acc]["balance"] = opening
    return rows


def inject_high_amount(state, acc, month):
    """
    Single high-value UK payment using GLOBAL_MERCHANTS (e.g. Amazon, Apple Store).
    """
    rows = []
    opening = state[acc]["balance"]
    ts = random_timestamp(TARGET_YEAR, month)

    # pick a global merchant that can support big amounts
    eligible = {k: v for k, v in GLOBAL_MERCHANTS.items() if v["max"] >= 500}
    merchant, rule = pick_merchant(eligible)

    upper = min(3000, opening * 2 if opening > 0 else 3000)
    amt = round(random.uniform(500, upper), 2)

    if amt <= opening:
        status = "Completed"
        closing = opening - amt
    else:
        status = "Declined"
        closing = opening

    rows.append(make_row(
        acc, ts, "Payment",
        opening, amt, closing, status,
        rule["category"], rule["channel"], rule["mode"],
        merchant, None, "UK"
    ))

    state[acc]["balance"] = closing
    return rows


def inject_night_high(state, acc, month):
    """
    High-value night-time online spend (00:00–04:00)
    using the full GLOBAL_MERCHANTS pool.
    """
    rows = []
    opening = state[acc]["balance"]
    ts = night_timestamp(TARGET_YEAR, month)

    # full global merchant pool (no hardcoding)
    merchant, rule = pick_merchant(GLOBAL_MERCHANTS)

    # high amount because it's a night-time fraud pattern
    amt = round(random.uniform(200, 2000), 2)

    if amt <= opening:
        status = "Completed"
        closing = opening - amt
    else:
        status = "Declined"
        closing = opening

    rows.append(make_row(
        acc, ts, "Payment",
        opening, amt, closing, status,
        rule["category"], rule["channel"], rule["mode"],
        merchant, None, "UK"   # night online transactions are UK-based
    ))

    state[acc]["balance"] = closing
    return rows


def inject_international_decline(state, acc, month):
    """
    Single foreign POS decline using INTL_MERCHANTS.
    """
    rows = []
    opening = state[acc]["balance"]
    country = random.choice(foreign_countries)
    ts = random_timestamp(TARGET_YEAR, month)

    merchant, rule = pick_merchant(INTL_MERCHANTS)

    # force decline: amount above balance
    amt = opening + random.uniform(20, 60)

    rows.append(make_row(
        acc, ts, "Payment",
        opening, amt, opening, "Declined",
        rule["category"], rule["channel"], rule["mode"],
        merchant, None, country
    ))
    # no balance change
    return rows


def inject_foreign_burst(state, acc, month):
    """
    2–4 foreign POS spends using INTL_MERCHANTS.
    Some may be completed, some declined depending on balance.
    """
    rows = []
    opening = state[acc]["balance"]
    base_ts = random_timestamp(TARGET_YEAR, month)
    count = random.randint(2, 4)

    for i in range(count):
        country = random.choice(foreign_countries)
        merchant, rule = pick_merchant(INTL_MERCHANTS)
        amt = round(random.uniform(rule["min"], rule["max"]), 2)
        ts = base_ts + timedelta(minutes=10 * i)

        if amt <= opening:
            status = "Completed"
            closing = opening - amt
        else:
            status = "Declined"
            closing = opening

        rows.append(make_row(
            acc, ts, "Payment",
            opening, amt, closing, status,
            rule["category"], rule["channel"], rule["mode"],
            merchant, None, country
        ))

        opening = closing
    state[acc]["balance"] = opening
    return rows


def inject_impossible_travel(state, acc, month):
    """
    UK POS spend followed within minutes by foreign POS (impossible travel).
    First leg: UK_MERCHANTS, second leg: INTL_MERCHANTS.
    """
    rows = []
    opening = state[acc]["balance"]

    # UK leg
    ts1 = random_timestamp(TARGET_YEAR, month)
    merchant_uk, rule_uk = pick_merchant(UK_MERCHANTS)
    loc_uk = home_city(acc)
    amt1 = 20.0

    if amt1 <= opening:
        status1 = "Completed"
        closing1 = opening - amt1
    else:
        status1 = "Declined"
        closing1 = opening

    rows.append(make_row(
        acc, ts1, "Payment",
        opening, amt1, closing1, status1,
        rule_uk["category"], rule_uk["channel"], rule_uk["mode"],
        merchant_uk, loc_uk, "UK"
    ))

    opening = closing1

    # foreign leg within minutes
    ts2 = ts1 + timedelta(minutes=random.randint(5, 25))
    merchant_foreign, rule_for = pick_merchant(INTL_MERCHANTS)
    country_for = random.choice(foreign_countries)
    amt2 = round(random.uniform(10, 50), 2)

    if amt2 <= opening:
        status2 = "Completed"
        closing2 = opening - amt2
    else:
        status2 = "Declined"
        closing2 = opening

    rows.append(make_row(
        acc, ts2, "Payment",
        opening, amt2, closing2, status2,
        rule_for["category"], rule_for["channel"], rule_for["mode"],
        merchant_foreign, None, country_for
    ))

    state[acc]["balance"] = closing2
    return rows


fraud_functions = [
    inject_repeated_declines,
    inject_back_to_back_retry,
    inject_high_velocity_small,
    inject_high_amount,
    inject_night_high,
    inject_international_decline,
    inject_foreign_burst,
    inject_impossible_travel,
]

# ------------------------------------------------------------
# 9. GENERATE ONE MONTH  Spark DataFrame
# ------------------------------------------------------------

def generate_month_sdf(month: int):
    rows = []
    state = {}

    # initialise state for internal accounts
    for _, r in accounts_pdf.iterrows():
        state[int(r["Account_Number"])] = {"balance": float(r["Current_Balance"])}

    total_rows = random.randint(MIN_TXNS, MAX_TXNS)
    fraud_count = int(total_rows * random.uniform(FRAUD_RATE_MIN, FRAUD_RATE_MAX))

    # normal transactions
    for _ in range(total_rows):
        acc, opening, _ = choose_or_create_account()
        if acc not in state:
            state[acc] = {"balance": opening}

        ts = random_timestamp(TARGET_YEAR, month)

        rnd = random.random()
        if rnd < HOME_CITY_SPEND_RATIO:
            country, location = "UK", home_city(acc)
        elif rnd < HOME_CITY_SPEND_RATIO + OTHER_UK_SPEND_RATIO:
            country, location = "UK", random.choice([c for c in uk_cities if c != home_city(acc)])
        else:
            country, location = random.choice(foreign_countries), None

        row, new_balance = generate_base_transaction(
            acc, state[acc]["balance"], ts, country, location
        )
        rows.append(row)
        state[acc]["balance"] = new_balance

    # fraud-like behaviour transactions
    for _ in range(fraud_count):
        acc = random.choice(list(state.keys()))
        func = random.choice(fraud_functions)
        rows.extend(func(state, acc, month))

    # create spark df
    sdf = spark.createDataFrame(rows, schema=schema)
    sdf = sdf.orderBy("Timestamp")
    return sdf

# ------------------------------------------------------------
# 10. YEAR LOOP + WRITE RAW CSV PER MONTH
# ------------------------------------------------------------

all_month_dfs = {}

for month in range(1, 13):
    print(f"\nGenerating RAW month {TARGET_YEAR}-{month:02d}")
    sdf_month = generate_month_sdf(month)
    all_month_dfs[month] = sdf_month
    print(f"   ✔ Rows: {sdf_month.count()}")

    #  use your abfss() helper here
#     out_path = abfss(f"/{TARGET_YEAR}_{month:02d}")  

#     (
#         sdf_month.coalesce(1)
#                  .write
#                  .option("header", "true")
#                  .mode("overwrite")
#                  .csv(out_path)
#     )
#     print(f"   ✔ Written RAW CSV → {out_path}")

print("\nSynthetic RAW data generation completed (Spark, new logic).")

# COMMAND ----------

# DBTITLE 1,each month as delta tables
# ============================================================
#  SAVE TRANSACTIONS TO DELTA TABLES
# ============================================================

for month, sdf in all_month_dfs.items():
    table_name = f"transactions_{TARGET_YEAR}_{month:02d}"
    (sdf.write.format("delta").mode("overwrite").saveAsTable(table_name))
    print(f"Saved {table_name} as Delta table.")


# COMMAND ----------

# DBTITLE 1,Export to ADLS
# ============================================================
#  EXPORT ALL DELTA TABLES TO ADLS GEN2 CONTAINER RAW
# ============================================================

for month in range(1, 13):
    month_str = f"{month:02d}"
    table_name = f"transactions_{TARGET_YEAR}_{month_str}"
    output_path = f"/{TARGET_YEAR}_{month_str}"

    print(f"\n Writing {table_name} to ADLS path: {output_path}")

    df = spark.table(table_name)
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .csv(abfss(output_path)))

    print(f" Exported {table_name} to ADLS.")
