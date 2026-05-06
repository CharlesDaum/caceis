from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = Path(__file__).resolve().parents[1]
DATASET_PATH = ROOT / "deliverables" / "Cleaned_Integrated_HR_Data.csv"
OUTPUT_DIR = ROOT / "prototype" / "data"
OUTPUT_PATH = OUTPUT_DIR / "prototype_data.json"

ABSENCE_THRESHOLD = 30.0
ABSENCE_ALERT_THRESHOLD = 0.55
PERFORMANCE_ALERT_THRESHOLD = 0.5

NUMERIC_FEATURES = [
    "Tenure_Years",
    "Total_Training_Hours",
    "Training_Sessions_Count",
    "Completed_Training_Sessions",
    "Review_Completed_Flag",
    "Dept_Median_Training",
    "Absorptive_Capacity_Index",
]
PERFORMANCE_NUMERIC_FEATURES = NUMERIC_FEATURES + [
    "Total_Absence_Days",
    "Absence_Count",
]
CAT_FEATURES = [
    "COUNTRY_GROUP_LABEL_EN",
    "SEXE_GROUP_LABEL_EN",
    "CONTRACT_GROUP_LABEL_EN",
    "Age range",
    "DEGREE_LEVEL_GROUP_LABEL_EN",
    "REASON_ENTRY_GROUP_LABEL_EN",
]
SEGMENT_FEATURES = [
    "Tenure_Years",
    "Total_Training_Hours",
    "Total_Absence_Days",
    "Review_Completed_Flag",
    "Absorptive_Capacity_Index",
]


def load_dataset() -> pd.DataFrame:
    df = pd.read_csv(DATASET_PATH)
    df["PERIOD"] = pd.to_datetime(df["PERIOD"], errors="coerce")
    for col in NUMERIC_FEATURES + ["Total_Absence_Days", "Absence_Count", "Performance_Score_Numeric"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def build_preprocessor(numeric_features: list[str], cat_features: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        [
            (
                "num",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]
                ),
                numeric_features,
            ),
            (
                "cat",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("encode", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                cat_features,
            ),
        ]
    )


def build_classifier(
    df: pd.DataFrame,
    target: str,
    numeric_features: list[str],
    cat_features: list[str],
) -> tuple[Pipeline, dict[str, float], pd.DataFrame]:
    X = df[numeric_features + cat_features].copy()
    y = df[target].astype(int)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )
    pipeline = Pipeline(
        [
            ("preprocess", build_preprocessor(numeric_features, cat_features)),
            ("model", LogisticRegression(max_iter=2500, class_weight="balanced")),
        ]
    )
    pipeline.fit(X_train, y_train)
    probabilities = pipeline.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    metrics = {
        "roc_auc": round(float(roc_auc_score(y_test, probabilities)), 3),
        "average_precision": round(float(average_precision_score(y_test, probabilities)), 3),
        "f1_at_0_5": round(float(f1_score(y_test, predictions)), 3),
        "support": int(len(df)),
        "positive_rate": round(float(y.mean()), 3),
    }
    scored = df.copy()
    scored[f"{target}_probability"] = pipeline.predict_proba(X)[:, 1]
    return pipeline, metrics, scored


def aggregate_feature_importance(
    pipeline: Pipeline,
    numeric_features: list[str],
    limit: int = 8,
) -> list[dict[str, float | str]]:
    preprocess = pipeline.named_steps["preprocess"]
    model = pipeline.named_steps["model"]
    feature_names = preprocess.get_feature_names_out()
    coefficients = model.coef_[0]
    grouped: dict[str, float] = {}
    for feature_name, coefficient in zip(feature_names, coefficients, strict=True):
        clean_name = feature_name.replace("num__", "").replace("cat__", "")
        root = clean_name if clean_name in numeric_features else clean_name.split("_", 1)[0]
        grouped[root] = grouped.get(root, 0.0) + abs(float(coefficient))
    ranked = sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:limit]
    return [{"feature": feature, "weight": round(weight, 3)} for feature, weight in ranked]


def build_segments(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, float | int | str]]]:
    scaled = StandardScaler().fit_transform(df[SEGMENT_FEATURES].fillna(0))
    model = KMeans(n_clusters=4, random_state=42, n_init=20)
    out = df.copy()
    out["segment_id"] = model.fit_predict(scaled)

    summaries = (
        out.groupby("segment_id", as_index=False)
        .agg(
            employees=("Employee_ID", "count"),
            avg_tenure=("Tenure_Years", "mean"),
            avg_training_hours=("Total_Training_Hours", "mean"),
            avg_absence_days=("Total_Absence_Days", "mean"),
            review_completion=("Review_Completed_Flag", "mean"),
            avg_performance=("Performance_Score_Numeric", "mean"),
        )
    )

    name_map: dict[int, str] = {}
    description_map: dict[int, str] = {}
    highest_training = int(summaries.sort_values("avg_training_hours", ascending=False).iloc[0]["segment_id"])
    highest_tenure = int(summaries.sort_values("avg_tenure", ascending=False).iloc[0]["segment_id"])
    highest_reviews = int(summaries.sort_values("review_completion", ascending=False).iloc[0]["segment_id"])

    for segment_id in summaries["segment_id"]:
        if int(segment_id) == highest_training:
            name_map[int(segment_id)] = "Overloaded Experts"
            description_map[int(segment_id)] = (
                "Highly trained employees with elevated absence pressure and strong review coverage."
            )
        elif int(segment_id) == highest_tenure:
            name_map[int(segment_id)] = "Tenured Core"
            description_map[int(segment_id)] = (
                "Long-tenure employees with stable performance but low recent capability refresh."
            )
        elif int(segment_id) == highest_reviews:
            name_map[int(segment_id)] = "Review-Ready Growers"
            description_map[int(segment_id)] = (
                "Employees already engaged in the review process and positioned for targeted development."
            )
        else:
            name_map[int(segment_id)] = "Under-Activated Talent"
            description_map[int(segment_id)] = (
                "Large population with low training exposure, low absence, and limited formal review activity."
            )

    out["segment"] = out["segment_id"].map(name_map)
    segment_cards: list[dict[str, float | int | str]] = []
    for row in summaries.to_dict(orient="records"):
        segment_id = int(row["segment_id"])
        segment_cards.append(
            {
                "segment": name_map[segment_id],
                "description": description_map[segment_id],
                "employees": int(row["employees"]),
                "avg_tenure": round(float(row["avg_tenure"]), 1),
                "avg_training_hours": round(float(row["avg_training_hours"]), 1),
                "avg_absence_days": round(float(row["avg_absence_days"]), 1),
                "review_completion": round(float(row["review_completion"]) * 100, 1),
                "avg_performance": round(float(row["avg_performance"]) if pd.notna(row["avg_performance"]) else 0.0, 2),
            }
        )
    segment_cards.sort(key=lambda item: item["employees"], reverse=True)
    return out, segment_cards


def build_peer_benchmarks(df: pd.DataFrame) -> pd.DataFrame:
    high_performers = df[df["Performance_Score_Numeric"].ge(4)].copy()
    peer = (
        high_performers.groupby(["COUNTRY_GROUP_LABEL_EN", "CONTRACT_GROUP_LABEL_EN"], dropna=False, as_index=False)
        .agg(
            peer_training_hours=("Total_Training_Hours", "median"),
            peer_absorptive_index=("Absorptive_Capacity_Index", "median"),
        )
    )
    country_fallback = (
        high_performers.groupby("COUNTRY_GROUP_LABEL_EN", dropna=False, as_index=False)
        .agg(
            country_peer_training_hours=("Total_Training_Hours", "median"),
            country_peer_absorptive_index=("Absorptive_Capacity_Index", "median"),
        )
    )
    out = df.merge(
        peer,
        on=["COUNTRY_GROUP_LABEL_EN", "CONTRACT_GROUP_LABEL_EN"],
        how="left",
    ).merge(country_fallback, on="COUNTRY_GROUP_LABEL_EN", how="left")
    global_training = float(high_performers["Total_Training_Hours"].median()) if not high_performers.empty else 7.0
    global_absorptive = (
        float(high_performers["Absorptive_Capacity_Index"].median()) if not high_performers.empty else 0.5
    )
    out["peer_training_hours"] = out["peer_training_hours"].fillna(out["country_peer_training_hours"]).fillna(global_training)
    out["peer_absorptive_index"] = (
        out["peer_absorptive_index"].fillna(out["country_peer_absorptive_index"]).fillna(global_absorptive)
    )
    out["training_gap_to_peer"] = (out["peer_training_hours"] - out["Total_Training_Hours"]).clip(lower=0)
    return out


def simulate_uplift(df: pd.DataFrame, performance_model: Pipeline) -> pd.DataFrame:
    simulated = df.copy()
    simulated["Review_Completed_Flag"] = 1
    simulated["Total_Training_Hours"] = np.maximum(
        simulated["Total_Training_Hours"],
        simulated["peer_training_hours"].clip(lower=7, upper=24),
    )
    simulated["Training_Sessions_Count"] = np.maximum(
        simulated["Training_Sessions_Count"],
        np.ceil(simulated["Total_Training_Hours"] / 8).astype(int),
    )
    simulated["Completed_Training_Sessions"] = np.maximum(
        simulated["Completed_Training_Sessions"],
        simulated["Training_Sessions_Count"],
    )
    simulated["Absorptive_Capacity_Index"] = (
        simulated["Total_Training_Hours"] / (simulated["Dept_Median_Training"].fillna(0) + 1)
    ).clip(0, 3)

    current_probability = performance_model.predict_proba(df[PERFORMANCE_NUMERIC_FEATURES + CAT_FEATURES])[:, 1]
    simulated_probability = performance_model.predict_proba(
        simulated[PERFORMANCE_NUMERIC_FEATURES + CAT_FEATURES]
    )[:, 1]
    out = df.copy()
    out["performance_probability"] = current_probability
    out["simulated_performance_probability"] = simulated_probability
    out["performance_uplift"] = out["simulated_performance_probability"] - out["performance_probability"]
    return out


def assign_risk_bands(series: pd.Series) -> pd.Series:
    return pd.cut(
        series,
        bins=[-0.01, 0.35, 0.65, 1.0],
        labels=["Low", "Medium", "High"],
    ).astype(str)


def build_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    recommendations: list[str] = []
    rationales: list[str] = []
    for row in out.itertuples(index=False):
        if row.absence_risk_probability >= 0.7 and row.segment == "Overloaded Experts":
            recommendations.append("Rebalance workload and open a wellbeing intervention")
            rationales.append("High predicted absence pressure is concentrated in a heavily trained, high-load segment.")
        elif row.Review_Completed_Flag < 1 and row.performance_uplift >= 0.08:
            recommendations.append("Close the review cycle and launch a 90-day coaching plan")
            rationales.append("The model estimates meaningful performance upside once review discipline is restored.")
        elif row.training_gap_to_peer >= 7 and row.performance_uplift >= 0.05:
            recommendations.append("Assign a targeted capability sprint matched to peer benchmarks")
            rationales.append("Comparable high performers in the same labor pool receive more training exposure.")
        elif row.segment == "Tenured Core" and row.Total_Training_Hours == 0:
            recommendations.append("Refresh tenured experts with cross-skilling and succession cover")
            rationales.append("Long tenure is stable, but capability refresh has fallen behind peer norms.")
        else:
            recommendations.append("Monitor quarterly and maintain current support plan")
            rationales.append("Current risk and uplift signals do not justify immediate escalation.")
    out["recommended_action"] = recommendations
    out["recommendation_rationale"] = rationales
    return out


def summarize_fairness(df: pd.DataFrame, group_col: str, probability_col: str, target_col: str) -> list[dict[str, float | int | str]]:
    grouped = (
        df.groupby(group_col, dropna=False, as_index=False)
        .agg(
            employees=("Employee_ID", "count"),
            avg_score=(probability_col, "mean"),
            observed_rate=(target_col, "mean"),
        )
        .sort_values("employees", ascending=False)
    )
    results: list[dict[str, float | int | str]] = []
    for row in grouped.head(8).to_dict(orient="records"):
        results.append(
            {
                "group": str(row[group_col]) if pd.notna(row[group_col]) else "Unknown",
                "employees": int(row["employees"]),
                "avg_score": round(float(row["avg_score"]) * 100, 1),
                "observed_rate": round(float(row["observed_rate"]) * 100, 1),
            }
        )
    return results


def top_country_summary(df: pd.DataFrame) -> list[dict[str, float | int | str]]:
    grouped = (
        df.groupby("COUNTRY_GROUP_LABEL_EN", as_index=False)
        .agg(
            headcount=("Employee_ID", "count"),
            avg_absence_risk=("absence_risk_probability", "mean"),
            avg_performance_upside=("performance_uplift", "mean"),
            review_completion=("Review_Completed_Flag", "mean"),
            no_training_share=("Total_Training_Hours", lambda s: (s <= 0).mean()),
        )
        .sort_values("headcount", ascending=False)
    )
    return [
        {
            "country": str(row["COUNTRY_GROUP_LABEL_EN"]),
            "headcount": int(row["headcount"]),
            "avg_absence_risk": round(float(row["avg_absence_risk"]) * 100, 1),
            "avg_performance_upside": round(float(row["avg_performance_upside"]) * 100, 1),
            "review_completion": round(float(row["review_completion"]) * 100, 1),
            "no_training_share": round(float(row["no_training_share"]) * 100, 1),
        }
        for row in grouped.head(8).to_dict(orient="records")
    ]


def build_priority_queue(df: pd.DataFrame) -> list[dict[str, float | int | str]]:
    priority = df.copy()
    priority["priority_score"] = (
        priority["absence_risk_probability"] * 0.5
        + priority["performance_uplift"].clip(lower=0) * 2.5
        + (1 - priority["Review_Completed_Flag"]) * 0.15
        + np.where(priority["training_gap_to_peer"] >= 7, 0.1, 0.0)
    )
    columns = [
        "Employee_ID",
        "COUNTRY_GROUP_LABEL_EN",
        "SEXE_GROUP_LABEL_EN",
        "CONTRACT_GROUP_LABEL_EN",
        "segment",
        "risk_band",
        "absence_risk_probability",
        "performance_probability",
        "performance_uplift",
        "Total_Absence_Days",
        "Total_Training_Hours",
        "training_gap_to_peer",
        "Review_Completed_Flag",
        "recommended_action",
        "recommendation_rationale",
        "priority_score",
    ]
    queue = priority.sort_values("priority_score", ascending=False).head(250)[columns]
    records: list[dict[str, float | int | str]] = []
    for row in queue.to_dict(orient="records"):
        records.append(
            {
                "employee_id": str(row["Employee_ID"]),
                "country": str(row["COUNTRY_GROUP_LABEL_EN"]),
                "gender": str(row["SEXE_GROUP_LABEL_EN"]),
                "contract": str(row["CONTRACT_GROUP_LABEL_EN"]),
                "segment": str(row["segment"]),
                "risk_band": str(row["risk_band"]),
                "absence_risk": round(float(row["absence_risk_probability"]) * 100, 1),
                "current_performance": round(float(row["performance_probability"]) * 100, 1),
                "uplift": round(float(row["performance_uplift"]) * 100, 1),
                "absence_days": round(float(row["Total_Absence_Days"]), 1),
                "training_hours": round(float(row["Total_Training_Hours"]), 1),
                "training_gap": round(float(row["training_gap_to_peer"]), 1),
                "review_completed": "Yes" if float(row["Review_Completed_Flag"]) >= 1 else "No",
                "action": str(row["recommended_action"]),
                "rationale": str(row["recommendation_rationale"]),
            }
        )
    return records


def build_employee_profiles(df: pd.DataFrame) -> list[dict[str, float | int | str]]:
    view_columns = [
        "Employee_ID",
        "COUNTRY_GROUP_LABEL_EN",
        "SEXE_GROUP_LABEL_EN",
        "CONTRACT_GROUP_LABEL_EN",
        "Age range",
        "ENTITY_LABEL_LOCAL",
        "segment",
        "risk_band",
        "absence_risk_probability",
        "performance_probability",
        "simulated_performance_probability",
        "performance_uplift",
        "Total_Absence_Days",
        "Total_Training_Hours",
        "peer_training_hours",
        "training_gap_to_peer",
        "Review_Completed_Flag",
        "Tenure_Years",
        "recommended_action",
        "recommendation_rationale",
    ]
    profiles = df.sort_values(["absence_risk_probability", "performance_uplift"], ascending=[False, False])[view_columns]
    output: list[dict[str, float | int | str]] = []
    for row in profiles.to_dict(orient="records"):
        output.append(
            {
                "employee_id": str(row["Employee_ID"]),
                "country": str(row["COUNTRY_GROUP_LABEL_EN"]),
                "gender": str(row["SEXE_GROUP_LABEL_EN"]),
                "contract": str(row["CONTRACT_GROUP_LABEL_EN"]),
                "age_range": str(row["Age range"]),
                "entity": str(row["ENTITY_LABEL_LOCAL"]),
                "segment": str(row["segment"]),
                "risk_band": str(row["risk_band"]),
                "absence_risk": round(float(row["absence_risk_probability"]) * 100, 1),
                "current_performance": round(float(row["performance_probability"]) * 100, 1),
                "simulated_performance": round(float(row["simulated_performance_probability"]) * 100, 1),
                "uplift": round(float(row["performance_uplift"]) * 100, 1),
                "absence_days": round(float(row["Total_Absence_Days"]), 1),
                "training_hours": round(float(row["Total_Training_Hours"]), 1),
                "peer_training_hours": round(float(row["peer_training_hours"]), 1),
                "training_gap": round(float(row["training_gap_to_peer"]), 1),
                "review_completed": "Yes" if float(row["Review_Completed_Flag"]) >= 1 else "No",
                "tenure_years": round(float(row["Tenure_Years"]), 1),
                "action": str(row["recommended_action"]),
                "rationale": str(row["recommendation_rationale"]),
            }
        )
    return output


def build_payload() -> dict[str, object]:
    df = load_dataset()
    df["high_absence_risk"] = (df["Total_Absence_Days"] >= ABSENCE_THRESHOLD).astype(int)

    absence_model, absence_metrics, absence_scored = build_classifier(
        df=df,
        target="high_absence_risk",
        numeric_features=NUMERIC_FEATURES,
        cat_features=CAT_FEATURES,
    )

    labeled_performance = df[df["Performance_Score_Numeric"].notna()].copy()
    labeled_performance["high_performer"] = labeled_performance["Performance_Score_Numeric"].ge(4).astype(int)
    performance_model, performance_metrics, _ = build_classifier(
        df=labeled_performance,
        target="high_performer",
        numeric_features=PERFORMANCE_NUMERIC_FEATURES,
        cat_features=CAT_FEATURES,
    )

    scored = absence_scored.copy()
    scored["performance_probability"] = performance_model.predict_proba(
        scored[PERFORMANCE_NUMERIC_FEATURES + CAT_FEATURES]
    )[:, 1]
    scored["risk_band"] = assign_risk_bands(scored["high_absence_risk_probability"])
    scored, segment_cards = build_segments(scored)
    scored = build_peer_benchmarks(scored)
    scored = simulate_uplift(scored, performance_model)
    scored["risk_band"] = assign_risk_bands(scored["high_absence_risk_probability"])
    scored = scored.rename(columns={"high_absence_risk_probability": "absence_risk_probability"})
    scored = build_recommendations(scored)

    filtered_priority = scored[
        (scored["absence_risk_probability"] >= ABSENCE_ALERT_THRESHOLD)
        | (scored["performance_uplift"] >= 0.05)
        | (scored["Review_Completed_Flag"] < 1)
    ].copy()

    headcount = int(len(scored))
    review_coverage = round(float(scored["Review_Completed_Flag"].mean()) * 100, 1)
    high_risk_population = int((scored["risk_band"] == "High").sum())
    avg_uplift = round(float(scored["performance_uplift"].clip(lower=0).mean()) * 100, 1)
    no_training_share = round(float((scored["Total_Training_Hours"] <= 0).mean()) * 100, 1)

    payload = {
        "meta": {
            "title": "CACEIS Human Capital AI Command Center",
            "subtitle": "Evolved prototype for HRBP prioritization, segmentation, and next-best-action planning",
            "source_dataset": str(DATASET_PATH.relative_to(ROOT)),
            "population": headcount,
            "last_period": scored["PERIOD"].max().strftime("%Y-%m-%d") if scored["PERIOD"].notna().any() else "Unknown",
        },
        "kpis": [
            {"label": "Employees in scope", "value": f"{headcount:,}", "detail": "Anonymized employee profiles available to the prototype"},
            {"label": "High absence alerts", "value": f"{high_risk_population:,}", "detail": "Employees currently scored in the high-risk band"},
            {"label": "Review coverage", "value": f"{review_coverage}%", "detail": "Share of employees with a completed review flag"},
            {"label": "Avg modeled upside", "value": f"{avg_uplift} pts", "detail": "Average gain in high-performer probability under recommended actions"},
            {"label": "No-training exposure", "value": f"{no_training_share}%", "detail": "Share of employees with zero recorded training hours"},
        ],
        "models": {
            "absence_risk": {
                "label": "Absence risk prediction",
                "objective": f"Predict employees likely to exceed {int(ABSENCE_THRESHOLD)} absence days.",
                "metrics": absence_metrics,
                "top_drivers": aggregate_feature_importance(absence_model, NUMERIC_FEATURES),
            },
            "performance_uplift": {
                "label": "Performance opportunity model",
                "objective": "Estimate who could move closer to high-performer benchmarks after review and training interventions.",
                "metrics": performance_metrics,
                "top_drivers": aggregate_feature_importance(performance_model, PERFORMANCE_NUMERIC_FEATURES),
            },
        },
        "segments": segment_cards,
        "fairness_checks": {
            "gender": summarize_fairness(scored, "SEXE_GROUP_LABEL_EN", "absence_risk_probability", "high_absence_risk"),
            "country": summarize_fairness(scored, "COUNTRY_GROUP_LABEL_EN", "absence_risk_probability", "high_absence_risk"),
        },
        "countries": top_country_summary(scored),
        "risk_distribution": [
            {
                "band": band,
                "employees": int(count),
                "share": round(float(count / headcount) * 100, 1),
            }
            for band, count in scored["risk_band"].value_counts().reindex(["High", "Medium", "Low"]).fillna(0).items()
        ],
        "filters": {
            "countries": sorted(scored["COUNTRY_GROUP_LABEL_EN"].dropna().astype(str).unique().tolist()),
            "segments": sorted(scored["segment"].dropna().astype(str).unique().tolist()),
            "risk_bands": ["High", "Medium", "Low"],
            "review_status": ["All", "Completed", "Missing"],
        },
        "priority_queue": build_priority_queue(filtered_priority),
        "employee_profiles": build_employee_profiles(scored),
    }
    return payload


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_payload()
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
