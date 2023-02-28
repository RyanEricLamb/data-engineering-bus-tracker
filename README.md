# An end-to-end data engineering project with Prefect, Docker, Terraform, Google CloudRun, BigQuery and Streamlit

<p align="center"><img src="rugby_app.gif" width="450"></p>

## Overview
A demo data engineering project to provide an overview of the key steps in going from raw data to a live web app. Uses UK Open Bus data to calculate late buses in realtime for any selected region.

### What is covered
- Creating and managing cloud resources with Terraform.
- Changing data ETL functions into tasks and flows that will be orchestrated with Prefect.
- Creating Prefect blocks to manage authentication and interaction with Google Cloud resources.
- Creating deployments to manage flow scheduling.
- Creating a Docker image for the script execution environment.
- Pushing this Docker image to Google Artifact Registry.
- Updating the Prefect deployment for Cloud Run to run the scripts.
- Setting up the Prefect agent on a Compute Instance.
- Creating a data visualisation app with Streamlit.

Read more here: 