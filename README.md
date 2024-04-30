# Data Flow Visualization, Connection and Understanding Tool for Data Pipelines

This project provides a visual representation of data pipelines, allowing users to identify sources, transformations, and targets within complex data flows. 

It includes scripts to create Excel tables, plot graphs, and utility functions that assist in replicating workflows and explaining connections, nodes, schema and field within the data.

## Table of Contents

- [Overview](#overview)
- [Data](#data)
- [Features](#features)
- [License](#license)

## Overview

The Data Flow Tool is designed to simplify the process of understanding and managing data pipelines.

By converting data flows into visual graphs, it enables clear communication of data processing steps from sources to targets, including all the transformations in between.

Other functions are provided to allow the user to obtain all the relevant info regarding the pipeline to take informed-decision and helping the thinking process

## Data

The input data are JSON file containing the pipeline to analyze. Accenture provided me 3 different json file with different level of difficulty:
- Easy: containing just a read table and a write table

![MicrosoftTeams-image (2)](https://github.com/emanueleiacca/Json-Pipeline-associate-Source-and-Destination-Table/assets/128679981/4d4d197d-0374-411c-9d98-ceeca1dac5e6)

- Medium: containing 2 read table and only a write table

![MicrosoftTeams-image (3)](https://github.com/emanueleiacca/Json-Pipeline-associate-Source-and-Destination-Table/assets/128679981/986f99c0-45b6-4531-9519-6194f3e98d68)

- Hard: containing one read table and 3 write table

![MicrosoftTeams-image (1)](https://github.com/emanueleiacca/Json-Pipeline-associate-Source-and-Destination-Table/assets/128679981/a4d5154c-5cf2-4dc8-9e98-0fe61f57f771)


## Features

- Visual graph generation from pipeline metadata.
- Script for creating Excel tables from data.
- Scripts for plotting data-related graphs.
- Utilities with helpfull function in the field of Data pipeline
- Explanations of pipeline nodes, schema, connections and fields.

## License

Project done for Accenture doing my internship, so the license goes to them
