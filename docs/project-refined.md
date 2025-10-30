# About exam project

## Instructions

- Find a suitable dataset
  - One or more files/tables/collections of data
  - Minimum 5GB of data
- Notify the teacher about the dataset and the main job that you mean to carry out
  - The execution plan of the main job must include **at least 2 shuffles**; some sample patterns are shown below
- Load the dataset on your S3 bucket
  - Include to be used for developing/debugging purposes
- When developing/debugging
  - Understand the dataset!
  - Use a sample of the dataset for debugging
  - Use a notebook to implement two versions of the agreed-upon job: a non-optimized one and an optimized one
- When deploying
  - Test both versions of the job and download the corresponding histories
  - Verify and understand the results (e.g., using Power BI, Tableau, Matplotlib, whichever you prefer)
  - **Important**: the code in the notebook should be optimized for its execution as if it was a production job; for instance:
    - Do not cache when not needed
    - Do not collect unnecessarily
- To deliver the project, notify the teacher via email and send a ZIP file with
  - The project, including both the notebook (with all the cells executed) and the application
  - The sample of the dataset
  - The history of the executed jobs
  - A link to directly download the full dataset
  - Any additional material (e.g., Power BI or Tableau file)

| Requirement                                                                | Completion |
| -------------------------------------------------------------------------- | ---------- |
| Find a suitable dataset                                                    | ✅          |
| Choose main job (i.e. how to transform data)                               | TODO       |
| Notify the teacher                                                         | TODO       |
| Load dataset on S3                                                         | TODO       |
| Develop jobs (opt and non-opt) in the notebook over sample data            | TODO       |
| Develop application for aws with both versions of the job and full dataset | TODO       |
| Run both jobs on AWS                                                       | TODO       |
| Download results (equals for both jobs) and histories (different per job)  | TODO       |
| Analyze results with matplotlib/Power BI/Tableau                           | TODO       |

## Datasets

- [NYC Taxi trips](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
