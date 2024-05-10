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

# SQL Query Analysis and Parsing Enhancements:

To meet the challenge of interpreting complex SQL queries embedded within JSON-defined data pipelines, we've developed a new parser that outclasses the other SQL Parser online on this particular task. These improvements facilitate to retrieve info regarding how data is manipulated and transferred across systems. Key features of the bespoke SQL parser include:

- Focused Parsing Strategy: The parser strategically focuses on the most inner parentheses within SQL queries. This approach is crucial for avoiding confusion caused by nested functions and complex SQL expressions. By targeting these innermost elements, the parser can extract essential column names without being misled by surrounding SQL syntax, ensuring that only relevant data is considered for analysis.
- Alias Resolution: A common challenge in SQL queries is the use of aliases, which can obscure the origins and destinations of data. Our parser effectively identifies these aliases and substitutes them with the actual table names from the database schema.
- Placeholder Management: Placeholders often represent dynamic elements within SQL queries, which can vary from one execution to another. Our parser not only detects these placeholders but also manages them by replacing the placeholder with its correct value and it also deal with values marked as "PROJDEF" with their actual values from an auxiliary configuration file. This feature is particularly useful in environments where configurations change frequently, as it allows the tool to adapt to new data contexts without manual intervention.

## Github and Library Information:

The generalized version of this SQL parser has been made available as an open-source library to benefit the wider community. Developers and data engineers can access this library to implement similar parsing capabilities in their own projects. For more details and to access the library, please visit the [SQLParserDataPipeline](https://github.com/emanueleiacca/SQLParserDataPipeline) project on GitHub and the library on [PyPI](https://pypi.org/project/SQLParserDataPipeline/).

## Current Limitations and Future Work:

- Scope of Parsing Capabilities: Currently, the enhanced parsing capabilities are optimized specifically for the 'hard' JSON file format, which includes the most complex and varied data flows. This focus was necessary due to the intricate structures and multiple data transformations typical in these files. We plan to extend these advanced parsing techniques to 'easy' and 'medium' difficulty levels through additional preprocessing steps, allowing for broader applicability of the tool across different pipeline complexities.

- The code for 'easy' and 'medium' difficulty levels instead, work perfectly fine for everyone, so we can say that we managed to generalize and iterate the problem.

- Integration with Data Flow Visualization Tool: The ultimate goal is to fully integrate these SQL parsing features into the main visualization tool. This integration will provide users with a comprehensive view of data lineage, from source to destination, including transformations at each step. By visualizing how SQL transformations manipulate data, users can gain insights into the efficiency and effectiveness of their data pipelines.


## License

Project done for Accenture doing my internship, so the license goes to them
