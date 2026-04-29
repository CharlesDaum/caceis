from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent


def lines(text: str) -> list[str]:
    body = dedent(text).strip("\n")
    return [f"{line}\n" for line in body.splitlines()]


def md_cell(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": lines(text),
    }


def code_cell(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": lines(text),
    }


NOTEBOOK_CELLS = [
    md_cell(
        """
        # CACEIS Human Capital Valuation
        ## Hybrid Data Pipeline: Preparation, KPI Engineering and Dashboard Export

        This notebook merges the ingestion and EDA pipeline from `1_Data_Pipeline_EDA_2.ipynb`
        with the KPI framing and AI-readiness guidance from
        `CACEIS_Human_Capital_Implementation.ipynb`.
        """
    ),
    code_cell(
        """
        import os
        import warnings
        from pathlib import Path

        import numpy as np
        import pandas as pd
        import plotly.express as px

        warnings.filterwarnings("ignore")
        pd.set_option("display.max_columns", 80)
        pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

        print("Libraries imported successfully.")
        """
    ),
    md_cell(
        """
        ## 1. Data Pipeline & Cleaning
        We load the four operational sources, standardize the employee identifier,
        aggregate the event-level tables, and merge everything into a dashboard-ready dataset.
        """
    ),
    code_cell(
        """
        cwd = Path.cwd().resolve()
        if (cwd / "HR Data").exists() and (cwd / "Training").exists():
            ROOT = cwd
        elif cwd.name == "deliverables" and (cwd.parent / "HR Data").exists():
            ROOT = cwd.parent
        else:
            ROOT = cwd

        hr_path = ROOT / "HR Data" / "Data.xlsx"
        perf_path = ROOT / "HR Data" / "2025 - Stats CACEIS EAE EP fichier de travail - Vretraitement.xlsx"
        abs_path = ROOT / "HR Data" / "20260121 - Absentéisme_-_détail_affectation_-_Bilan_social 2025.xlsx"
        train_path = ROOT / "Training" / "Training_Records_Unnamed.xlsx"

        df_hr = pd.read_excel(hr_path)
        df_perf = pd.read_excel(perf_path)
        df_abs = pd.read_excel(abs_path)
        df_train = pd.read_excel(train_path)

        print(f"HR Master: {df_hr.shape[0]} rows")
        print(f"Performance (EAE): {df_perf.shape[0]} rows")
        print(f"Absenteeism: {df_abs.shape[0]} rows")
        print(f"Training: {df_train.shape[0]} rows")
        """
    ),
    md_cell(
        """
        ### Data Integration
        We harmonize `Employee_ID`, aggregate absenteeism and training at employee level,
        then left-join the enrichment tables onto the HR master file.
        """
    ),
    code_cell(
        """
        def standardize_employee_id(df: pd.DataFrame, rename_map: dict[str, str]) -> pd.DataFrame:
            df = df.rename(columns=rename_map).copy()
            if "Employee_ID" in df.columns:
                df["Employee_ID"] = df["Employee_ID"].astype(str).str.strip()
            return df


        df_hr = standardize_employee_id(df_hr, {"ID Employee": "Employee_ID"})
        df_perf = standardize_employee_id(df_perf, {" IUG": "Employee_ID"})
        df_abs = standardize_employee_id(df_abs, {"Employee Code": "Employee_ID"})
        df_train = standardize_employee_id(df_train, {"Employee Code": "Employee_ID"})

        if "PERIOD" in df_hr.columns:
            df_hr["PERIOD"] = pd.to_datetime(df_hr["PERIOD"], errors="coerce")
            df_hr = (
                df_hr.sort_values(["Employee_ID", "PERIOD"])
                .drop_duplicates(subset=["Employee_ID"], keep="last")
                .reset_index(drop=True)
            )

        absence_days_col = next(
            (
                col
                for col in ["Jours Ouvrés Absence", "Jours Ouvrables Absence", "Jour Calendaires Absence"]
                if col in df_abs.columns
            ),
            None,
        )
        if absence_days_col is None:
            raise KeyError("No absence day column found in absenteeism file.")

        df_abs[absence_days_col] = pd.to_numeric(df_abs[absence_days_col], errors="coerce")
        df_abs_agg = (
            df_abs.groupby("Employee_ID", as_index=False)
            .agg(
                Total_Absence_Days=(absence_days_col, "sum"),
                Absence_Count=(absence_days_col, "count"),
            )
        )

        df_train["Total_Training_Hours"] = pd.to_numeric(df_train["Total_Training_Hours"], errors="coerce")
        df_train["Total_Training_Hours"] = df_train["Total_Training_Hours"].clip(lower=0)
        if "Status" in df_train.columns:
            df_train["Training_Completed_Flag"] = (
                df_train["Status"].astype(str).str.strip().str.lower().eq("réalisé")
            )
        else:
            df_train["Training_Completed_Flag"] = True

        df_train_agg = (
            df_train.groupby("Employee_ID", as_index=False)
            .agg(
                Total_Training_Hours=("Total_Training_Hours", "sum"),
                Training_Sessions_Count=("Session_ID", "nunique"),
                Completed_Training_Sessions=("Training_Completed_Flag", "sum"),
            )
        )

        if "Note de performance" in df_perf.columns:
            df_perf["Performance_Score_Numeric"] = pd.to_numeric(df_perf["Note de performance"], errors="coerce")
        else:
            df_perf["Performance_Score_Numeric"] = np.nan

        if " Statut du document" in df_perf.columns:
            df_perf["Review_Completed_Flag"] = (
                df_perf[" Statut du document"].astype(str).str.contains("termin", case=False, na=False)
            ).astype(int)
        elif "Statut" in df_perf.columns:
            df_perf["Review_Completed_Flag"] = (
                df_perf["Statut"].astype(str).str.strip().str.lower().eq("terminé")
            ).astype(int)
        else:
            df_perf["Review_Completed_Flag"] = 0

        perf_cols = [
            "Employee_ID",
            "Review_Completed_Flag",
            "Performance_Score_Numeric",
            "Note de performance",
            " Libellé Organisation niveau 06",
            "Type contrat",
            " Année",
        ]
        perf_cols = [col for col in perf_cols if col in df_perf.columns]

        df_perf_agg = (
            df_perf[perf_cols]
            .sort_values(
                by=["Review_Completed_Flag", "Performance_Score_Numeric"],
                ascending=[False, False],
                na_position="last",
            )
            .drop_duplicates(subset=["Employee_ID"])
        )

        df_merged = df_hr.merge(df_perf_agg, on="Employee_ID", how="left")
        df_merged = df_merged.merge(df_abs_agg, on="Employee_ID", how="left")
        df_merged = df_merged.merge(df_train_agg, on="Employee_ID", how="left")

        for col in [
            "Total_Absence_Days",
            "Absence_Count",
            "Total_Training_Hours",
            "Training_Sessions_Count",
            "Completed_Training_Sessions",
            "Review_Completed_Flag",
        ]:
            if col in df_merged.columns:
                df_merged[col] = df_merged[col].fillna(0)

        print(f"Final integrated dataset: {df_merged.shape[0]} rows x {df_merged.shape[1]} columns")
        df_merged.head(3)
        """
    ),
    code_cell(
        """
        data_quality = pd.DataFrame(
            {
                "Missing_Count": df_merged.isna().sum(),
                "Missing_Pct": (df_merged.isna().mean() * 100).round(2),
            }
        ).sort_values(["Missing_Pct", "Missing_Count"], ascending=False)

        data_quality
        """
    ),
    md_cell(
        """
        ## KPI logic
        1. Workforce capacity and inclusion: headcount trend and female representation.
        2. Performance governance: completion rate and score distribution for annual reviews.
        3. Capability building: completed training hours, satisfaction, and perceived impact.
        4. Well-being risk: worked absence days per employee and absence mix.
        5. Value creation efficiency: net banking income and personnel cost per FTE.
        """
    ),
    code_cell(
        """
        ref_date = pd.Timestamp("2025-12-31")

        if "DATE_ENTRY_CACEIS" in df_merged.columns:
            df_merged["DATE_ENTRY_CACEIS"] = pd.to_datetime(df_merged["DATE_ENTRY_CACEIS"], errors="coerce")
            df_merged["Tenure_Years"] = (ref_date - df_merged["DATE_ENTRY_CACEIS"]).dt.days / 365.25
        else:
            df_merged["Tenure_Years"] = np.nan

        df_merged["Org_Fragility_Risk"] = np.where(
            (df_merged["Tenure_Years"] > 10) & (df_merged["Total_Absence_Days"] > 15),
            "High Risk",
            "Normal",
        )

        department_col = "ENTITY_LABEL_LOCAL" if "ENTITY_LABEL_LOCAL" in df_merged.columns else "COUNTRY_GROUP"
        df_merged["Dept_Median_Training"] = (
            df_merged.groupby(department_col)["Total_Training_Hours"].transform("median")
        )
        df_merged["Absorptive_Capacity_Index"] = (
            df_merged["Total_Training_Hours"] / (df_merged["Dept_Median_Training"] + 1)
        ).clip(0, 3)

        # KPI 1: Performance governance
        performance_governance_summary = (
            df_merged.groupby(department_col, dropna=False)
            .agg(
                Employees=("Employee_ID", "nunique"),
                Review_Completion_Rate=("Review_Completed_Flag", "mean"),
                Avg_Performance_Score=("Performance_Score_Numeric", "mean"),
            )
            .reset_index()
            .sort_values("Review_Completion_Rate", ascending=False)
        )
        performance_governance_summary["Review_Completion_Rate"] = (
            performance_governance_summary["Review_Completion_Rate"] * 100
        ).round(2)
        performance_governance_summary["Avg_Performance_Score"] = (
            performance_governance_summary["Avg_Performance_Score"].round(2)
        )

        # KPI 2: Cross KPI - training impact on performance
        df_merged["Training_Intensity_Band"] = pd.cut(
            df_merged["Total_Training_Hours"],
            bins=[-0.01, 0, 7, 20, np.inf],
            labels=["0h", "0.1-7h", "7-20h", "20h+"],
        )

        dept_no_training_baseline = (
            df_merged.loc[df_merged["Total_Training_Hours"] == 0]
            .groupby(department_col)["Performance_Score_Numeric"]
            .median()
            .rename("Dept_No_Training_Baseline")
        )
        df_merged = df_merged.merge(dept_no_training_baseline, on=department_col, how="left")
        df_merged["Training_Performance_Delta"] = (
            df_merged["Performance_Score_Numeric"] - df_merged["Dept_No_Training_Baseline"]
        )

        training_perf_summary = (
            df_merged.groupby("Training_Intensity_Band", dropna=False, observed=False)
            .agg(
                Employee_Count=("Employee_ID", "nunique"),
                Avg_Training_Hours=("Total_Training_Hours", "mean"),
                Avg_Performance_Score=("Performance_Score_Numeric", "mean"),
                Avg_Performance_Delta=("Training_Performance_Delta", "mean"),
            )
            .reset_index()
        )
        training_perf_summary[["Avg_Training_Hours", "Avg_Performance_Score", "Avg_Performance_Delta"]] = (
            training_perf_summary[["Avg_Training_Hours", "Avg_Performance_Score", "Avg_Performance_Delta"]].round(2)
        )

        print("Performance governance summary")
        display(performance_governance_summary.head(10))

        print("Training vs performance summary")
        display(training_perf_summary)

        df_merged[
            [
                "Employee_ID",
                department_col,
                "Tenure_Years",
                "Total_Absence_Days",
                "Total_Training_Hours",
                "Review_Completed_Flag",
                "Performance_Score_Numeric",
                "Org_Fragility_Risk",
                "Absorptive_Capacity_Index",
                "Training_Performance_Delta",
            ]
        ].head()
        """
    ),
    md_cell(
        """
        ## 3. Exploratory Data Analysis (EDA)
        These charts validate the analytical signal in the merged dataset before dashboarding
        and before any downstream machine learning work.
        """
    ),
    code_cell(
        """
        fig = px.scatter(
            df_merged,
            x="Total_Training_Hours",
            y="Total_Absence_Days",
            color="Org_Fragility_Risk",
            hover_data=[department_col, "Performance_Score_Numeric"],
            title="Absorptive Capacity vs Latency (Training vs Absenteeism)",
            opacity=0.6,
            trendline="ols",
        )
        fig.show()
        """
    ),
    code_cell(
        """
        fig2 = px.histogram(
            df_merged,
            x="Tenure_Years",
            color=department_col,
            title="Tenure Distribution by Entity (Input for Random Survival Forests)",
            barmode="stack",
            nbins=30,
        )
        fig2.show()
        """
    ),
    code_cell(
        """
        corr_vars = [
            "Tenure_Years",
            "Total_Training_Hours",
            "Total_Absence_Days",
            "Performance_Score_Numeric",
        ]
        corr_df = df_merged[corr_vars].apply(pd.to_numeric, errors="coerce")
        corr_matrix = corr_df.corr()

        fig_corr = px.imshow(
            corr_matrix,
            text_auto=".2f",
            color_continuous_scale="RdBu_r",
            zmin=-1,
            zmax=1,
            title="Correlation Heatmap of Core Numerical Variables",
        )
        fig_corr.update_layout(width=800, height=600)
        fig_corr.show()
        """
    ),
    md_cell(
        """
        ## Preliminary Findings
        - The merge confirms heterogeneous data completeness across HR, EAE, absenteeism and training, which justifies explicit quality controls before any predictive modeling.
        - Tenure, absence days and training hours show materially different distributions across entities, suggesting non-linear patterns and segment effects rather than a single global relationship.
        - Review completion and performance scores are not uniformly distributed across organizational units, which supports governance monitoring and fairness checks prior to model deployment.
        - The interaction between training exposure, performance outcomes and absenteeism points to potential confounding, which supports future use of causal-oriented methods such as Double ML, while tenure-based risk trajectories remain a strong fit for Random Survival Forests.
        """
    ),
    md_cell(
        """
        ## Governance and AI readiness
        - Privacy: analysis stays at anonymized employee ID level.
        - Bias controls: country and gender segment checks are retained for fairness diagnostics.
        - Prototype direction: rule-based KPI monitoring plus predictive risk scoring for absenteeism/performance coverage gaps.
        """
    ),
    md_cell(
        """
        ## 5. Export Final
        The cleaned and enriched dataset is exported as a CSV file for downstream Streamlit dashboard ingestion.
        """
    ),
    code_cell(
        """
        output_path = ROOT / "deliverables" / "Cleaned_Integrated_HR_Data.csv"
        df_merged.to_csv(output_path, index=False)
        print(f"Successfully exported cleaned data to {output_path}")
        """
    ),
]


NOTEBOOK = {
    "cells": NOTEBOOK_CELLS,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.14",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    output_path = root / "deliverables" / "Final_Hybrid_Human_Capital_Pipeline.ipynb"
    output_path.write_text(json.dumps(NOTEBOOK, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote notebook to {output_path}")


if __name__ == "__main__":
    main()
