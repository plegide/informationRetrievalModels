# informationRetrievalModels

## Description

This project implements an **Information Retrieval (IR)** system in two parts, organized into two directories:
- **`mri-webindexer-badillo-legide-main`**: Implements a desktop search system with multi-threaded indexing and document analysis tools.
- **`mri-searcher-badillo-legide-main`**: Focuses on indexing, searching, and evaluating queries over the **NPL Collection**, including advanced retrieval models.

Both parts utilize **Apache Lucene** for indexing and searching capabilities.


## Features

### `mri-webindexer-badillo-legide-main`
- **Multi-threaded Indexing**:
  - Supports parallel indexing for improved efficiency.
  - Processes folders in parallel, creates partial indexes, and merges them.
- **Indexed Fields**:
  - File metadata: path, size, type, creation, modification, and access timestamps.
  - Custom fields: hostname, thread name, Lucene-compatible timestamps.
- **Configuration**:
  - A `config.properties` file allows filtering by file extensions and limiting indexed lines.
- **Additional Utilities**:
  - **TopTermsInDocs**: Extract and rank terms from documents based on TF-IDF.
  - **RemoveDuplicates**: Identify and remove duplicate documents from the index.

### `mri-searcher-badillo-legide-main`
- **NPL Collection Indexing**:
  - Indexes document IDs and content with specified retrieval models and analyzers.
- **Query Evaluation**:
  - Metrics: Precision@N, Recall@N, MRR, MAP, and their averages.
  - Outputs results to text and CSV files.
- **Parameter Optimization**:
  - Trains and tests retrieval models to find optimal parameters.
- **Statistical Testing**:
  - Compares model performance using t-tests or Wilcoxon tests.
- **Optional Dense Retrieval**:
  - Implements Dense Retrieval models for further comparison.

---

## Usage

### Dependencies
- Java (version 8 or later)
- Apache Lucene
- Apache Commons Math
- Maven

### Compilation
Navigate to the respective directory (`mri-webindexer-badillo-legide-main` or `mri-searcher-badillo-legide-main`) and build using Maven:
```
mvn clean install
```

### Execution
Run the JAR files generated in the `target` directory. Below are examples of usage:

#### `mri-webindexer-badillo-legide-main`

1. **IndexFiles**:
  ```
    java -jar target/IndexFiles.jar -index <index_path> -docs <docs_path> -numThreads 4
  ```
2. **TopTermsInDoc**:
  ```
    java -jar target/TopTermsInDocs.jar -index <index_path> -docID 1-10 -top 5
  ```
3. **RemoveDuplicates**:
  ```
    java -jar target/RemoveDuplicates.jar -index <index_path> -out <output_index_path>
  ```

#### `mri-searcher-badillo-legide-main`

1. **IndexNPL***
  ```
    java -jar target/IndexNPL.jar -openmode create -index <index_path> -docs <docs_path> -indexingmodel jm 0.2
  ```

2. **SearchEvalNPL**
  ```
    java -jar target/SearchEvalNPL.jar -search jm 0.2 -index <index_path> -queries all -cut 10 -top 5
  ```

3. **TrainingTestNPL**
  ```
    java -jar target/TrainingTestNPL.jar -evaljm 1-20 21-30 -cut 10 -metric MAP -indexin <index_path>
  ```

4. **Compare**
  ```
    java -jar target/Compare.jar -test t 0.05 -results result1.csv result2.csv
  ```

### Outputs
  - Indexes: Stored in the specified output folder.
  - Evaluation Results: Text and CSV files summarizing query performance and metrics.
  - Statistical Tests: Significance results (p-values) for comparing models.
