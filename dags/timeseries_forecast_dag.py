from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# ─── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "ghazi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="timeseries_forecast_pipeline",
    default_args=default_args,
    description="📈 Daily time-series forecasting (linear regression)",
    schedule="@daily",
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["forecast", "timeseries"],
) as dag:

    start = EmptyOperator(task_id="start")

    @task
    def extract() -> str:
        """Generate a toy time series and write to CSV."""
        rng = pd.date_range(end=datetime.today(), periods=60, freq="D")
        df = pd.DataFrame({
            "ds": rng,
            "y": (rng.dayofyear * 0.1 + (rng.day % 10) * 0.5)
        })
        path = "/tmp/ts_data.csv"
        df.to_csv(path, index=False)
        return path

    @task
    def preprocess(csv_path: str) -> tuple[str, list[dict]]:
        """
        Read CSV, create 1-day lag, split into train/test,
        save train to CSV, serialize test as JSON-friendly list.
        """
        df = pd.read_csv(csv_path, parse_dates=["ds"])
        # use .bfill() instead of fillna(method="bfill")
        df["lag_1"] = df["y"].shift(1).bfill()

        train_df = df.iloc[:-10]
        test_df = df.iloc[-10:].copy()

        # 1) Save train for the train task
        train_path = "/tmp/train.csv"
        train_df.to_csv(train_path, index=False)

        # 2) Serialize test_df as list-of-dicts with ISO dates
        records = test_df.to_dict(orient="records")
        for rec in records:
            rec["ds"] = rec["ds"].isoformat()

        return train_path, records

    @task
    def train(train_path: str) -> str:
        """Train on the lag feature and save model params."""
        df_train = pd.read_csv(train_path, parse_dates=["ds"])
        X = df_train[["lag_1"]]
        y = df_train["y"]

        model = LinearRegression()
        model.fit(X, y)

        coef = float(model.coef_[0])
        intercept = float(model.intercept_)

        model_path = "/tmp/model_params.txt"
        with open(model_path, "w") as f:
            f.write(f"{coef},{intercept}")

        return model_path

    @task
    def evaluate(model_path: str, test_records: list[dict]) -> float:
        """Rebuild model, load test_records, compute and return MSE."""
        # Re-create test DataFrame
        test_df = pd.DataFrame(test_records)
        test_df["ds"] = pd.to_datetime(test_df["ds"])

        coef, intercept = [
            float(x) for x in open(model_path).read().split(",")
        ]

        y_true = test_df["y"]
        y_pred = test_df["lag_1"] * coef + intercept

        mse = mean_squared_error(y_true, y_pred)
        return mse

    @task
    def report(mse: float):
        """Log the metric (could also send alerts here)."""
        print(f"Model MSE on last 10 days: {mse:.3f}")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    # ─── Wiring ───────────────────────────────────────────────────────────────────
    data_path = extract()
    train_path, test_records = preprocess(data_path)
    model_path = train(train_path)
    mse = evaluate(model_path, test_records)
    report_task = report(mse)

    start >> data_path >> train_path >> model_path >> mse >> report_task >> end
