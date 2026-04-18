import json
import os
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
from constructs import Construct


class SalesPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        alert_email: str = self.node.try_get_context("alertEmail")
        error_threshold: str = str(self.node.try_get_context("errorThreshold") or "0.20")
        athena_owner_user_arn = self.node.try_get_context("athenaResultsOwnerArn")

        # ── SNS Alert Topic ───────────────────────────────────────────────
        alert_topic = sns.Topic(
            self,
            "AlertTopic",
            topic_name="shopmart-pipeline-alerts",
        )
        alert_topic.add_subscription(subscriptions.EmailSubscription(alert_email))

        query_results_topic = sns.Topic(
            self,
            "QueryResultsTopic",
            topic_name="shopmart-query-results",
        )
        query_results_topic.add_subscription(subscriptions.EmailSubscription(alert_email))

        # ── Raw Ingestion Bucket ──────────────────────────────────────────
        raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            bucket_name=f"shopmart-raw-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="archive-raw-to-glacier",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        )
                    ],
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ── Processed + Errors Output Bucket ─────────────────────────────
        processed_bucket = s3.Bucket(
            self,
            "ProcessedBucket",
            bucket_name=f"shopmart-processed-{self.account}-{self.region}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            lifecycle_rules=[
                # Automatically expire error files after 30 days
                s3.LifecycleRule(
                    id="expire-error-files",
                    prefix="errors/",
                    expiration=Duration.days(30),
                ),
                # Move processed Parquet to Infrequent Access after 60 days
                s3.LifecycleRule(
                    id="ia-processed-parquet",
                    prefix="processed/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(60),
                        )
                    ],
                ),
            ],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ── Athena Query Results Bucket ───────────────────────────────────
        athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name=f"shopmart-athena-results-{self.account}-{self.region}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-athena-query-results",
                    prefix="athena-results/",
                    expiration=Duration.days(30),
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        if athena_owner_user_arn:
            athena_owner_principal = iam.ArnPrincipal(athena_owner_user_arn)
            athena_results_bucket.grant_read_write(athena_owner_principal)

        # ── Lambda IAM Role (least privilege) ────────────────────────────
        lambda_role = iam.Role(
            self,
            "ProcessorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        raw_bucket.grant_read(lambda_role)
        processed_bucket.grant_write(lambda_role)
        alert_topic.grant_publish(lambda_role)

        notifier_role = iam.Role(
            self,
            "AthenaResultNotifierRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        athena_results_bucket.grant_read(notifier_role)
        query_results_topic.grant_publish(notifier_role)

        athena_scheduler_role = iam.Role(
            self,
            "AthenaDailySchedulerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        # Scheduler needs read access to source Parquet data and write access
        # to Athena query output files.
        processed_bucket.grant_read(athena_scheduler_role)
        athena_results_bucket.grant_read_write(athena_scheduler_role)
        athena_scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                ],
                resources=["*"],
            )
        )

        # ── Lambda Function ───────────────────────────────────────────────
        # AWS-managed layer: AWSSDKPandas-Python312 (pandas + pyarrow)
        # ARN format: arn:aws:lambda:<region>:336392948345:layer:AWSSDKPandas-Python312:<version>
        pandas_layer_arn = (
            self.node.try_get_context("pandasLayerArn")
            or f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python312:14"
        )
        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, "PandasLayer", pandas_layer_arn
        )

        src_path = os.path.join(os.path.dirname(__file__), "..", "..", "src")

        processor_fn = lambda_.Function(
            self,
            "ProcessorFn",
            function_name="shopmart-sales-processor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="processor.lambda_handler",
            code=lambda_.Code.from_asset(src_path),
            layers=[pandas_layer],
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "PROCESSED_BUCKET": processed_bucket.bucket_name,
                "RAW_BUCKET": raw_bucket.bucket_name,
                "ERROR_THRESHOLD": error_threshold,
                "SNS_TOPIC_ARN": alert_topic.topic_arn,
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
            retry_attempts=2,  # Built-in async retry (BR-8)
        )

        athena_result_notifier_fn = lambda_.Function(
            self,
            "AthenaResultNotifierFn",
            function_name="shopmart-athena-result-notifier",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="athena_result_notifier.lambda_handler",
            code=lambda_.Code.from_asset(src_path),
            role=notifier_role,
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "SNS_TOPIC_ARN": query_results_topic.topic_arn,
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        athena_daily_scheduler_fn = lambda_.Function(
            self,
            "AthenaDailySchedulerFn",
            function_name="shopmart-athena-daily-scheduler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="athena_daily_scheduler.lambda_handler",
            code=lambda_.Code.from_asset(src_path),
            role=athena_scheduler_role,
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "ATHENA_DATABASE": "shopmart_sales",
                "ATHENA_RESULTS_BUCKET": athena_results_bucket.bucket_name,
                "ATHENA_RESULTS_PREFIX": "athena-results/daily-cron/",
                "ATHENA_SQL_FILE": "/var/task/athena_daily_queries.sql",
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # ── S3 raw upload -> Lambda processor (immediate) ─────────────────
        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(processor_fn),
            s3.NotificationKeyFilter(prefix="raw/", suffix=".csv"),
        )

        # ─── OLD: EventBridge daily cron (before switching to immediate S3 trigger) ───
        # from aws_cdk import aws_events as events, aws_events_targets as targets
        # daily_rule = events.Rule(
        #     self,
        #     "DailyProcessorRule",
        #     schedule=events.Schedule.cron(hour="8", minute="5"),  # 08:05 UTC daily
        # )
        # daily_rule.add_target(targets.LambdaFunction(processor_fn))
        # ─── Replaced with: S3 event notification above (sub-second latency) ───────

        # ── Athena result files (.csv) -> notifier Lambda -> SNS ──────────
        athena_results_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(athena_result_notifier_fn),
            s3.NotificationKeyFilter(prefix="athena-results/", suffix=".csv"),
        )

        # ── EventBridge cron (daily) -> Athena batch scheduler ─────────
        athena_daily_rule = events.Rule(
            self,
            "AthenaDailyCronRule",
            schedule=events.Schedule.cron(minute="30", hour="8"),
            description="Runs named Athena analytics queries daily at 08:30 UTC",
        )
        athena_daily_rule.add_target(targets.LambdaFunction(athena_daily_scheduler_fn))

        # ── Glue Database (for Athena discoverability — BR-6) ─────────────
        glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="shopmart_sales",
                description="ShopMart processed sales data",
                location_uri=f"s3://{processed_bucket.bucket_name}/processed/good_data/",
            ),
        )

        # Glue Table: explicit canonical schema for good_data.
        # The crawler only discovers new partitions and inherits these table types.
        good_data_table = glue.CfnTable(
            self,
            "GoodDataTable",
            catalog_id=self.account,
            database_name="shopmart_sales",
            table_input=glue.CfnTable.TableInputProperty(
                name="good_data",
                description="ShopMart processed sales data - Parquet, partitioned by store and date",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "EXTERNAL": "TRUE",
                    "parquet.compress": "SNAPPY",
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="order_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="customer_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="product_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="order_date", type="string"),
                        glue.CfnTable.ColumnProperty(name="quantity", type="double"),
                        glue.CfnTable.ColumnProperty(name="unit_price", type="double"),
                        glue.CfnTable.ColumnProperty(name="payment_status", type="string"),
                    ],
                    location=f"s3://{processed_bucket.bucket_name}/processed/good_data/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={"serialization.format": "1"},
                    ),
                    stored_as_sub_directories=False,
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="store", type="string"),
                    glue.CfnTable.ColumnProperty(name="date", type="string"),
                ],
            ),
        )
        good_data_table.add_dependency(glue_db)

        glue_crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        processed_bucket.grant_read(glue_crawler_role)

        glue.CfnCrawler(
            self,
            "SalesCrawler",
            name="shopmart-good-data-crawler",  # discovers new partition folders only
            role=glue_crawler_role.role_arn,
            database_name="shopmart_sales",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{processed_bucket.bucket_name}/processed/good_data/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(10 8 * * ? *)"
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY",
            ),
            configuration=json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
                },
                "Grouping": {
                    "TableGroupingPolicy": "CombineCompatibleSchemas",
                },
            }),
        )

        # Glue Crawler: infers metadata schema from JSON files automatically.
        glue.CfnCrawler(
            self,
            "MetadataCrawler",
            name="shopmart-metadata-crawler",
            role=glue_crawler_role.role_arn,
            database_name="shopmart_sales",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{processed_bucket.bucket_name}/metadata/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(10 8 * * ? *)"
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY",
            ),
        )

        # ── CloudWatch Alarm: Lambda Errors ───────────────────────────────
        lambda_error_alarm = cloudwatch.Alarm(
            self,
            "LambdaErrorAlarm",
            alarm_name="shopmart-processor-errors",
            alarm_description="Lambda processor threw unhandled errors",
            metric=processor_fn.metric_errors(
                period=Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=3,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        lambda_error_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── CloudWatch Alarm: Lambda P99 Duration ─────────────────────────
        cloudwatch.Alarm(
            self,
            "LambdaDurationAlarm",
            alarm_name="shopmart-processor-duration-p99",
            alarm_description="Lambda P99 latency approaching 5-minute timeout",
            metric=processor_fn.metric_duration(
                period=Duration.minutes(5),
                statistic="p99",
            ),
            threshold=240_000,  # 240 seconds — 80% of 5-minute timeout
            evaluation_periods=2,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
