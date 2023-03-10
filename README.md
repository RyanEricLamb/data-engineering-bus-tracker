# An end-to-end data engineering project with Prefect, Docker, Terraform, Google CloudRun, BigQuery and Streamlit

<p align="center"><img src="app_demo.png"></p>

## Overview
A demo data engineering project covering the key steps to go from raw data to a live web app. Uses UK Open Bus Data to calculate late buses in real-time for any selected region.

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

Full project walkthrough [here](https://medium.com/@ryanelamb/a-data-engineering-project-with-prefect-docker-terraform-google-cloudrun-bigquery-and-ae37f7314ebb?source=friends_link&sk=a2abdd0138ad08949d31bb0e03b3639c)