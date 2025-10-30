# Project Proposal

## Dataset

Name: NYC Taxi & Limousine Commission (TLC) Trip Record Data
Source link: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page?utm_source=chatgpt.com)

## Main Job

**Objective**: Analyze how tip generosity varies by pickup location and time of day, and identify the top pickup zones with the most generous passengers.

### Plan

#### Setup

- clean up and select relevant columns
- create derived columns (tip percentage, hour of day)

#### Shuffles

- join zones id with zone lookup table
- aggregate per location and hour
- aggregate per average tip percentage
- order by average tip percentage
