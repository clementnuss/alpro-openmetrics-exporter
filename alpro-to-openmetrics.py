# %%
import pandas as pd
import sqlite3, pytz, argparse, requests
from datetime import datetime, timedelta, date

parser = argparse.ArgumentParser(
    prog="MilkData importer",
    description="Imports milk production data from Alpro systems, and converts it to openmetrics format",
)
parser.add_argument("--filename", default="alpro.db")
parser.add_argument(
    "--daily", action="store_true", help="imports today's data only", default=True
)
parser.add_argument(
    "--history",
    action="store_true",
    help="imports historical data until yesterday",
    default=False,
)
# args = parser.parse_args("--filename ./alpro.db --daily".split(" "))
args = parser.parse_args()

tz = pytz.timezone("Europe/Zurich")


def parse_duration(dur: str) -> int:
    t = datetime.strptime(dur, "%H:%M:%S")
    return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second).seconds


def convert_to_openmetrics(row: pd.Series, metric_name: str):
    ts: pd.Timestamp = row["timestamp"]
    ts = int(ts.timestamp()) * 1000
    value = row["value"]
    labels = ""
    for label in row.axes[0]:
        if label != "timestamp" and label != "value":
            labels += f'{label}="{row[label]}",'
    return f"{metric_name}{{{labels}}} {float(value)} {ts}"


# %%
# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("./alpro.db")
dates_fmt = {"MilkDateTime": "%Y-%m-%d %H:%M:%S", "RecDate": "%Y-%m-%d %H:%M:%S"}

cow_detail = pd.read_sql_query("SELECT * from TblCow", con)
cow_names = cow_detail.set_index("CowNo")["CowID"]

metrics = pd.DataFrame()

# %%
if args.history:
    cow_milk_30d = pd.read_sql_query(
        "SELECT * from TblCowLast30DayMilk", con, parse_dates=dates_fmt
    )
    cow_milk_30d["Duration"] = cow_milk_30d.Duration.apply(
        lambda s: parse_duration(s.split()[1])
    )
    cow_milk_30d = cow_milk_30d[cow_milk_30d["MilkDateTime"].notna()]
    cow_milk_30d["MilkDateTime"] = cow_milk_30d["MilkDateTime"].apply(
        lambda t: tz.localize(t)
    )
    cow_milk_30d["day"] = cow_milk_30d["MilkDateTime"].dt.date
    cow_milk_30d["cow_id"] = cow_milk_30d["CowNo"].apply(lambda id: cow_names[id])
    cow_milk_30d["cow_no"] = cow_milk_30d["CowNo"]
    cow_milk_30d["session"] = cow_milk_30d["Session"]
    cow_milk_30d["timestamp"] = cow_milk_30d["MilkDateTime"]

    # # historical data
    # cow_daily_feed = pd.read_sql_query(
    #     "SELECT * from TblCowDailyFeed", con, parse_dates=dates_fmt
    # )

    time_series = {
        "cow_milk_yield": "Yield",
        "cow_milk_peak_flow": "PeakFlow",
        "cow_milk_avg_flow": "AverageFlow",
        "cow_milk_duration": "Duration",
    }
    cols = ["cow_id", "cow_no", "timestamp", "session"]
    for metric_name, col in time_series.items():
        df = cow_milk_30d[cols + [col]]
        metrics = pd.concat(
            [
                metrics,
                df.rename(columns={col: "value"}).apply(
                    lambda row: convert_to_openmetrics(row, metric_name), axis=1
                ),
            ]
        )

    # compute daily yield, by summing session 1, 1+2, 1+2+3, 1+2+3+4
    for session in range(1, 4):
        metric_name = "cow_milk_daily_yield"
        df = cow_milk_30d[cow_milk_30d["Session"] <= session]
        sum = df.groupby(["day", "CowNo"])["Yield"].sum()
        df = cow_milk_30d.merge(
            sum, how="left", on=["CowNo", "day"], suffixes=("", f"_Daily")
        )
        df = df[df["Session"] == session]
        cols = ["cow_id", "cow_no", "timestamp", "session", "Yield_Daily"]
        df = df[cols]
        metrics = pd.concat(
            [
                metrics,
                df.rename(columns={"Yield_Daily": "value"}).apply(
                    lambda row: convert_to_openmetrics(row, metric_name), axis=1
                ),
            ]
        )

# %%
if args.daily:
    # current data (current working day and previous day)
    TblCow = pd.read_sql_query("SELECT * from TblCow", con, parse_dates=dates_fmt)

    TblCow["cow_no"] = TblCow["CowNo"]
    TblCow["cow_id"] = TblCow["CowNo"].apply(lambda id: cow_names[id])
    TblCow["Yield_Daily"] = 0.0

    for session in range(1, 4):
        df = TblCow[f"MilkTimeToday{session}"]
        df = (
            df[df.notna()]
            .apply(lambda s: s.split()[1])
            .apply(lambda t: datetime.strptime(t, "%H:%M:%S"))
            .apply(lambda t: datetime.combine(date.today(), t.time()))
            .apply(lambda t: tz.localize(t))
            .rename("timestamp")
        )
        TblCow = TblCow.join(df)
        TblCow["session"] = session

        durCol = f"Duration{session}"
        duration = TblCow[durCol]
        duration = duration[duration.notna()].apply(
            lambda s: parse_duration(s.split()[1])
        )
        TblCow[durCol] = duration

        TblCow["Yield_Daily"] += TblCow[f"MilkToday{session}"]
        already_milked = TblCow[f"MilkTimeToday{session}"].notna()
        cols = ["cow_id", "cow_no", "timestamp", "session"]
        df = TblCow[already_milked]

        time_series = {
            "cow_milk_yield": f"MilkToday{session}",
            "cow_milk_peak_flow": f"PeakFlow{session}",
            "cow_milk_avg_flow": f"AverFlow{session}",
            "cow_milk_duration": f"Duration{session}",
            "cow_milk_daily_yield": f"Yield_Daily",
        }

        for metric_name, col_name in time_series.items():
            df = df[cols].join(TblCow[col_name].rename("value"))
            # df = df[cols]

            metrics = pd.concat(
                [
                    metrics,
                    df.apply(
                        lambda row: convert_to_openmetrics(row, metric_name), axis=1
                    ),
                ]
            )

        TblCow = TblCow.drop(columns=["session", "timestamp"])

# %%
# url = "http://vmagent-vmagent:8429/insert/2900/prometheus/api/v1/import/prometheus"

if len(metrics) > 0:
    print(metrics[0])
    url = "http://vmagent-vmagent:8429/insert/2900/prometheus/api/v1/import/prometheus"
    requests.post(
        f"{url}/?extra_label=retention_period=long-term&extra_label=job=milk_data_v1.0.1",
        data=metrics[0].str.cat(sep="\n"),
    )

# %%
con.close()  # close sqlite DB
