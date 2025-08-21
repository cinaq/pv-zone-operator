# GitHub Actions Workflows

This directory contains GitHub Actions workflows for CI/CD processes.

## CI Workflow (`ci.yaml`)

Runs on every push to `main` branch and on pull requests targeting `main`.

Features:
- Runs Go formatting checks
- Runs Go code verification (`go vet`)
- Runs all tests
- Builds the Docker image (without pushing)

## Release Workflow (`release.yaml`)

Runs when a tag with format `v*` is pushed (e.g., `v1.0.0`).

Features:
- Runs tests to verify the release
- Creates a GitHub Release
- Builds and pushes the Docker image to DockerHub with two tags:
  - `latest`
  - The version number from the tag (e.g., `1.0.0`)

## Required Secrets

For the release workflow to function properly, you need to add the following secrets to your GitHub repository:

- `DOCKERHUB_USERNAME`: Your DockerHub username
- `DOCKERHUB_TOKEN`: A DockerHub access token (not your password)

You can add these secrets in your GitHub repository under Settings > Secrets and variables > Actions.
