#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.pipeline_stack import SalesPipelineStack

app = cdk.App()

account = app.node.try_get_context("account") or os.getenv("CDK_DEFAULT_ACCOUNT")
region = app.node.try_get_context("region") or os.getenv("CDK_DEFAULT_REGION")

SalesPipelineStack(
    app,
    "SalesPipeline",
    env=cdk.Environment(
        account=account,
        region=region,
    ),
    description="ShopMart automated sales data pipeline",
)

app.synth()
