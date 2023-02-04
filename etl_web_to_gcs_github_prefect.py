from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs_github import data_to_bucket  # <-- my ETL script

github_block = GitHub.load("dtc-prefect-gh")

github_deployment = Deployment.build_from_flow(
    flow=data_to_bucket,
    name="web-to-gc-bucket-gh",
    storage=github_block,
    parameters={"color": "green", "year": 2020, "months": [11]},
)

github_deployment.apply()
