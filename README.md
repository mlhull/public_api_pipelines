# Public API Pipelines

## Table of Contents
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Public APIs](#public-apis)

## Description

This repository contains multiple ETL examples that source data from public APIs:
  - Ticketmaster pipeline curates data on upcoming events located in a defined market
  - Spotify pipeline curates data on top tracks associated with an artist of interest

Each pipeline uses Apache Airflow ('Airflow') and Python applications to extract data from sources, handle HTTP exceptions, transform data, and load it to AWS S3 targets.

## Features
- **Airflow Orchestration**: Utilize Airflow to schedule and manage your ETL workflows. This includes leveraging Airflow's Xcoms to transfer data between tasks and SimpleHTTPOperator to handle API calls (see [Spotify pipeline](https://github.com/mlhull/public_api_pipelines/tree/main/spotify_tracks))
- **API Authentication**: Handle use cases requiring API authentication to securely access data.

## Installation
1. Install Airflow. I recommend using Astro's Docker Image, as it ensures you're using the latest version of Airflow.
2. Establish a config file that can be accessed within your Airflow Instance. I placed mine in my 'include' folder - a folder that's automatically accessible my instance's docker container. If you want to place it elsewhere and make it accessible to Airflow you will need to mount the location as a new volume within the docker-compose.yaml file.
3. Create AWS service account with permissions to interact S3.
    - Ensure the bucket is set up in S3 to receive output files. This should be captured in Airflow as an environment variable.
4. Ensure there is a connection between AWS Cloud Services and your Airflow instance.
5. Obtain access from source data systems to make API calls and set up connections in Aiwflow:
    - Ticketmaster: The API is token-based. See [here](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/) for API documentation and details on obtaining access. To note, the pipeline in this repository used the Discovery API. You will need to set up 1 connection in Airflow for this pipeline to get the data (GET request).
    - Spotify: The pipeline featured in this repository uses the Client Credential authorization workflow to interact with the Spotify API. See [here](https://developer.spotify.com/documentation/web-api/concepts/authorization) for API documentation and details on obtaining access. To note, you will set up 2 connections in Airflow for this pipeline: 1 to get the token (POST request) and 1 to get the data (GET request).
6. Download the pipeline dag files into your Airflow dag folder, and the [api_etl_module file](https://github.com/mlhull/public_api_pipelines/blob/main/api_etl_module)
    - [Ticketmaster pipeline files](https://github.com/mlhull/public_api_pipelines/tree/main/ticketmaster_events)
    - [Spotify pipeline files](https://github.com/mlhull/public_api_pipelines/tree/main/spotify_tracks)

## Usage
1. Trigger the DAG in Apache Airflow to start the pipeline. Make sure any application files containing custom functions are imported to the DAG. 
2. Data received by GET API call will be staged using a s3 bucket. Processed API data will be saved in the target S3 bucket.

## Public APIs
1. [Ticketmaster Discovery API](https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/)
2. [Spotify API](https://developer.spotify.com/documentation/web-api)



