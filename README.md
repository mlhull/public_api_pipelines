# Public API Pipelines

## Table of Contents
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Public APIs](#public-apis)

## Description

This repository contains multiple ETL examples that source data from public APIs. Each pipeline uses Apache Airflow and Python applications to extract data from sources, handle HTTP exceptions, transform data, and load it to AWS S3 targets.

## Features
- **Airflow Orchestration**: Utilize Apache Airflow to schedule and manage your ETL workflows.
- **API Authentication**: Handle use cases requiring API authentication to securely access data.

## Installation
- Install Airflow. I recommend using Astro's Docker Image because, as one of many benefits, this ensures you're using the last instance of Airflow.
- Create AWS service account with permissions to interact S3.
- Ensure there is a connection between AWS Cloud Services and your Airflow instance.
- Obtain access from source data systems to make API calls:
  -- Ticketmaster: The API is token-based. See [here](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/) for API documentation and details on obtaining access. To note, the pipeline in this repository used the Discovery API.

## Usage
1. Trigger the DAG in Apache Airflow to start the pipeline. Make sure any application files containing custom functions are imported to the DAG. 
2. Processed API data will be saved in the target S3 bucket.

## Public APIs
1. [Ticketmaster Discovery API](https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/)



