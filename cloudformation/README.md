# Test infrastructure

[github_actions_oidc.yaml](github_actions_oidc.yaml) defines the AWS infrastructure used by PyAthena's integration tests and GitHub Actions.
It includes GitHub OIDC authentication and IAM roles, Athena SQL and Spark workgroups, an S3 staging bucket, and S3 Tables resources with Glue catalog integration.

The template parameters configure the GitHub repository, resource names, and an optional existing OIDC provider.
See the [testing guide](../docs/testing.md#github-actions) for initial stack creation.

The benchmark-specific template is maintained separately in [benchmarks/cloudformation/](../benchmarks/cloudformation/).

## Update an existing stack

Use the AWS CLI locally with credentials that can update the stack and its resources.
Run these commands from the repository root, replacing `your-aws-profile` with the profile for the test account.
Adjust the region and stack name if the stack was created with different settings.

```bash
export AWS_PROFILE=your-aws-profile
export AWS_DEFAULT_REGION=us-west-2

aws cloudformation describe-stacks \
  --stack-name github-actions-oidc-pyathena \
  --query 'Stacks[0].{Id:StackId,Status:StackStatus,Parameters:Parameters}'
```

Check the selected stack and its current parameters, then apply the local template:

```bash
aws cloudformation deploy \
  --stack-name github-actions-oidc-pyathena \
  --template-file cloudformation/github_actions_oidc.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset
```

The [`deploy` command](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy.html) applies the update and waits for completion.
It retains existing parameter values when `--parameter-overrides` is omitted; add that option only for parameters being changed or newly required parameters without defaults.
`CAPABILITY_NAMED_IAM` is required for the named IAM roles in this template.
An unchanged template and parameters produce a successful no-op.
If an update fails, inspect the stack events:

```bash
aws cloudformation describe-stack-events \
  --stack-name github-actions-oidc-pyathena
```
