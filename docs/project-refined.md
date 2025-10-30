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

| Requirement                       | Completion |
| --------------------------------- | ---------- |
| Find a suitable dataset           | ✅          |
| Choose main job                   | TODO       |
| Notify the teacher                | TODO       |
| Load dataset on S3                | TODO       |
| Develop jobs                      | TODO       |
| Deploy jobs                       | TODO       |
| Track histories                   | TODO       |
| Analyze histories with matplotlib | TODO       |

## Datasets

- [NYC Taxi trips](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Main job patterns

Here are some typical job patterns to get an execution plan with at least 2 shuffles. Consider the MovieLens dataset used in Lab 104.

- *Join and aggregate*: this is trivial if your dataset is composed by multiple files/tables; for instance:
  - Join movies with ratings
  - Aggregate to calculate the average rating of each genre
- *Double aggregation*: do a first pre-aggregation on two (or more) attributes, then further aggregating on a subset of those attributes; for instance:
  - Aggregate ratings by movieid and year, to get the average rating of each movie in each year
  - Aggregate by year to calculate the average of the average ratings for each year (which could be interpreted as the "average quality" of the movies seen every year)
- *Self-join*: do a first aggregation on one attribute, then join the result with the original dataset and re-aggregate on a different attribute; for instance:
  - Aggregate by movieid to calculate the average rating and the number of ratings for each movie, then calculate a "Class" attribute as follows
    - If the count is <10, classify the movie as "Insufficient ratings"
    - If the count is >=10 and the average rating is <=2, classify the movie as "Bad"
    - If the count is >=10 and the average rating is >2 and <=4, classify the movie as "Average"
  - If the count is >=10 and the average rating is >4, classify the movie as "Good"
  - Join the result with the ratings on the movieid
  - Aggregate by class and year to see how many ratings are give each year on the different classes of movies
