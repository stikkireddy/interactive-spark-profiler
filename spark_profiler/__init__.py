from contextlib import contextmanager
from pyspark.sql import functions as F, SparkSession
import plotly.express as px
import pandas as pd
import time


class StageMetricsCharts:
    @staticmethod
    def get_sunburst(databrics_metrics: 'SparkMetricsListener'):
        StageMetricsCharts._sunburst_df_to_fig(
            StageMetricsCharts._get_agg_stage_sunburst_df(*databrics_metrics._step_agg_metrics.collect()))

    @staticmethod
    def _get_agg_stage_sunburst_df_for_row(row):
        step_name = row["step_name"]
        executor_run_time = row["executorRunTime"]
        executor_cpu_time = row["executorCpuTime"]
        executor_deserialize_time = row["executorDeserializeTime"]
        result_serialization_time = row["resultSerializationTime"]
        jvm_gc_time = row["jvmGCTime"]
        shuffle_fetch_wait_time = row["shuffleFetchWaitTime"]
        shuffle_write_time = row["shuffleWriteTime"]

        other = "Other"
        other_execution_time = (executor_run_time - executor_cpu_time - jvm_gc_time -
                                shuffle_fetch_wait_time - shuffle_write_time)

        execution_run_time_components = ["JVM GC", "Shuffle Fetch Wait Time", "Shuffle Write Time", "Executor CPU Time",
                                         other, None, None]
        execution = [*(["Executor Run Time"] * 5), "Executor Deserialize Time", "Results Serialize Time"]
        steps = [step_name] * 7
        duration = [jvm_gc_time, shuffle_fetch_wait_time, shuffle_write_time, executor_cpu_time, other_execution_time,
                    executor_deserialize_time, result_serialization_time]
        return pd.DataFrame(
            dict(execution=execution, steps=steps, execution_run_time_components=execution_run_time_components,
                 duration=duration)
        )

    @staticmethod
    def _get_agg_stage_sunburst_df(*rows):
        return pd.concat([StageMetricsCharts._get_agg_stage_sunburst_df_for_row(row.asDict()) for row in rows])

    @staticmethod
    def _sunburst_df_to_fig(df):
        fig = px.sunburst(df, path=['steps', 'execution', 'execution_run_time_components'], values='duration')
        import plotly.graph_objects as go
        fig2 = go.Figure(go.Sunburst(
            labels=list(fig['data'][0]['labels']),
            parents=list(fig['data'][0]['parents']),
            values=list(fig['data'][0]['values']),
            ids=list(fig['data'][0]['ids']),
            textinfo='label+percent entry',
            branchvalues="total")
        )
        fig2.show()


class SparkMetricsListener:
    def __init__(self, display, spark: SparkSession = None, skip=False):
        self._skip = skip
        # TODO: dynamically get this if running in notebook by fetching this from ipython globals
        self._display_method = display
        self._spark = spark or SparkSession.getActiveSession()
        self._all_metrics = None
        self._step_agg_metrics = None
        self._accumulator_metrics = None

    @staticmethod
    def add_metadata(df, **kwargs):
        meta_data_cols = [F.lit(v).alias(k) for k, v in kwargs.items()]
        return df.select(*meta_data_cols, "*")

    def display(self):
        self._display_method(self._all_metrics)
        self._display_method(self._step_agg_metrics)

    def execution_sunburst(self):
        StageMetricsCharts.get_sunburst(self)

    @contextmanager
    def add_step(self, step_name, metrics_wait_secs=15):

        if self._skip is True:
            yield
            return

        from sparkmeasure import StageMetrics
        # Code to acquire resource, e.g.:
        stagemetrics = StageMetrics(self._spark)
        stagemetrics.begin()

        try:
            yield
        finally:
            # Code to release resource, e.g.:
            time.sleep(metrics_wait_secs)
            stagemetrics.end()
            metadata = {"step_name": step_name}

            stage_metrics = self.add_metadata(stagemetrics.create_stagemetrics_DF(), **metadata)
            if self._all_metrics is None:
                self._all_metrics = stage_metrics
            else:
                self._all_metrics = self._all_metrics.union(stage_metrics)

            agg_metrics = self.add_metadata(stagemetrics.aggregate_stagemetrics_DF(), **metadata)
            if self._step_agg_metrics is None:
                self._step_agg_metrics = agg_metrics
            else:
                self._step_agg_metrics = self._step_agg_metrics.union(agg_metrics)

