# DATAENG 300 — Homework 3: MapReduce in PySpark

This project applies MapReduce concepts using PySpark to complete two major tasks:

1. Compute tf-idf scores for a collection of news articles.
2. Evaluate the objective function for a linear Support Vector Machine (SVM) using custom MapReduce logic.

## Part 1: tf-idf on AG News Data

We used PySpark RDD transformations to calculate tf-idf values from the AG News dataset. This involved:
- Computing term frequency (TF) for each word in each document.
- Computing document frequency (DF) across the corpus.
- Applying the tf-idf formula manually with MapReduce logic.
The final tf-idf results were added as a new column and previewed in the output.

## Part 2: SVM Objective and Prediction with MapReduce

Using distributed processing, we evaluated the soft-margin SVM loss function:
- Input data: `data_for_svm.csv`, `w.csv`, and `bias.csv`.
- The loss function includes both hinge loss and L2 regularization:
  
  Objective = (1/n) * sum(max(0, 1 - yᵢ(wᵗxᵢ + b))) + λ * ||w||²

- We used RDD `.map()` and `.reduce()` functions to compute:
  - The average hinge loss across all rows.
  - The regularization term using the L2 norm of the weight vector.
- Predictions were generated using the sign of (wᵗx + b).

## Technologies

- Python 3
- PySpark (RDD API only — no MLlib)

## Notes

- All calculations and predictions were implemented manually using RDD logic to follow a MapReduce paradigm.
- This homework reinforced Spark fundamentals and showed how large-scale machine learning computations can be broken down into basic operations.

