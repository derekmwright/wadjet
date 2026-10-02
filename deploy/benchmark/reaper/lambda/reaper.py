"""Terminate wadjet-bench EC2 instances that have been running too long.

Deployed as a Lambda on a 30-minute EventBridge schedule. Safety net to
prevent benchmark instances from running indefinitely when auto-shutdown
or `tofu destroy` fails.

deploy/gates/terraform deploys the same code a second time as
wadjet-gate-reaper (TAG_KEY=Name, TAG_VALUE=wadjet-gate-runner,
MAX_RUNTIME_MINUTES=40, ADR-0046). MAX_RUNTIME_MINUTES, when set, wins over
MAX_RUNTIME_HOURS.
"""

import boto3
import os
import time


def handler(event, context):
    ec2 = boto3.client("ec2")
    max_minutes = os.environ.get("MAX_RUNTIME_MINUTES")
    if max_minutes:
        max_seconds = int(max_minutes) * 60
    else:
        max_seconds = int(os.environ.get("MAX_RUNTIME_HOURS", "2")) * 3600
    limit = f"{max_seconds / 3600:.2f}h"
    tag_key = os.environ.get("TAG_KEY", "Project")
    tag_value = os.environ.get("TAG_VALUE", "wadjet-bench")

    response = ec2.describe_instances(
        Filters=[
            {"Name": f"tag:{tag_key}", "Values": [tag_value]},
            {"Name": "instance-state-name", "Values": ["pending", "running"]},
        ]
    )

    now = time.time()
    to_terminate = []

    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            launch_time = instance["LaunchTime"].timestamp()
            runtime_hours = (now - launch_time) / 3600

            name = ""
            for tag in instance.get("Tags", []):
                if tag["Key"] == "Name":
                    name = tag["Value"]
                    break

            if (now - launch_time) > max_seconds:
                print(
                    f"TERMINATING {instance_id} ({name}): "
                    f"running {runtime_hours:.2f}h (limit: {limit})"
                )
                to_terminate.append(instance_id)
            else:
                print(f"OK {instance_id} ({name}): running {runtime_hours:.2f}h")

    if to_terminate:
        ec2.terminate_instances(InstanceIds=to_terminate)
        print(f"Terminated {len(to_terminate)} instances: {to_terminate}")
        return {"terminated": to_terminate}

    print("No instances to terminate")
    return {"terminated": []}
