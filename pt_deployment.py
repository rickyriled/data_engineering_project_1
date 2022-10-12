# deployment.py

from pt import pushpullflow
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=pushpullflow,
    name="push-pull-pipe",
    parameters={},
    schedule=(CronSchedule(cron="0 0/1 * * *", timezone="America/Chicago")),
    infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="test",
)

if __name__ == "__main__":
    deployment.apply()
