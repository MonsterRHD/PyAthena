# Test infrastructure

[github_actions_oidc.yaml](github_actions_oidc.yaml) defines the AWS infrastructure used by PyAthena's integration tests and GitHub Actions.
It includes GitHub OIDC authentication and IAM roles, Athena SQL and Spark workgroups, an S3 staging bucket, and S3 Tables resources with Glue catalog integration.

The template parameters configure the GitHub repository, resource names, and an optional existing OIDC provider.
See the [testing guide](../docs/testing.md) for setup and deployment instructions.

The benchmark-specific template is maintained separately in [benchmarks/cloudformation/](../benchmarks/cloudformation/).
