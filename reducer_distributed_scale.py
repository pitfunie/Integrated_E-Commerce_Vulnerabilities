"""
Distributed Processing Reducer with PySpark

This script reads term-posting pairs from a distributed file system (or local file),
groups postings by term, and outputs the merged posting list for each term using Spark's distributed processing.

Input format (one per line):
    term<TAB>docID:pos

How it works:
- Reads the input as an RDD (Resilient Distributed Dataset).
- Splits each line into (term, posting) pairs.
- Groups postings by term using reduceByKey.
- Outputs each term and its merged posting list.

Example Usage (from terminal):
    spark-submit reducer_spark.py input.txt output_dir

You must have Spark installed and configured to use this script.
"""

from pyspark import SparkContext
import sys


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


def main(input_path, output_path):
    # Create a SparkContext, which is the entry point for Spark functionality.
    sc = SparkContext(appName="DistributedReducer")

    # 1. Read the input file as an RDD (Resilient Distributed Dataset), where each element is a line from the file.
    lines = sc.textFile(input_path)

    # 2. Parse each line into a (term, posting) pair by splitting on the first tab character.
    #    Example: "apple\t1:2" becomes ("apple", "1:2")
    pairs = lines.map(lambda line: line.strip().split("\t", 1))  # (term, posting)

    # 3. Group postings by term and merge postings into a comma-separated string:
    #    - mapValues: Wrap each posting in a list, so ("apple", "1:2") becomes ("apple", ["1:2"])
    #    - reduceByKey: For each term, concatenate all posting lists together.
    #    - map: Format the output as "term<TAB>posting1,posting2,..."
    merged = (
        pairs.mapValues(lambda posting: [posting])
        .reduceByKey(lambda a, b: a + b)
        .map(lambda kv: f"{kv[0]}\t{','.join(kv[1])}")
    )

    # 4. Save the result to the output directory (Spark will create one file per partition).
    merged.saveAsTextFile(output_path)

    # 5. Stop the SparkContext to free up resources.
    sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit reducer_spark.py <input_path> <output_path>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
