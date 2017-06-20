# Sparkling Water Development Documentation

This documentation acts only as the table of content and provides links to the actual documentation documents.

- [Typical Use Case](#UseCase)
- [Requirements](#Req)
- [Design](#Design)
    - [Basic Primitives](doc/basic_primitives.rst)
    - [Supported Platforms](doc/supported_platforms.rst)  
    - [Spark Frame - H2O Frame Mapping](doc/spark_h2o_mapping.rst)
- [Features](#Features)
  - [Supported Data Sources](#DataSource)
  - [Supported Data Formats](#DataFormat)
  - [Data Sharing](#DataShare)
- [Running Sparkling Water](doc/run.rst)
- **Sparkling Water Configuration**
    - [Sparkling Water Configuration Properties](doc/configuration_properties.rst)
    - [Memory Allocation](doc/memory_setup.rst)
    - [Sparkling Water Internal Backend Tuning](doc/internal_backend_tuning.rst)
- **Tutorials**    
    - [Enabling Security](doc/security.rst)
    - [Calling H2O Algorithms](doc/calling_h2o_algos.rst)
    - Frames Conversions & Creation
        - [Spark Frame - H2O Frame Conversions](doc/spark_h2o_conversions.rst)
        - [H2O Frame as Spark's Data Source](doc/datasource.rst)
        - [Create H2OFrame From an Existing Key](doc/h2o_frame_from_key.rst)
    - Logging
        - [Change Sparkling Shell Logging Level](doc/change_log_level.rst)
        - [Obtain Sparkling Water Logs](doc/log_location.rst)
- **Testing**
    - [Running Unit Tests](doc/unit_tests.rst)
    - [Integration Tests](doc/integ_tests.rst)
- [Sparkling Water and Zeppelin](doc/zeppelin.rst)

--- 
 
<a name="Design"></a>
## Design

Sparkling Water is designed to be executed as a regular Spark application.
It provides a way to initialize H2O services on each node in the Spark cluster and access
data stored in data structures of Spark and H2O.

Since Sparkling Water is designed as Spark application, it is launched 
inside a Spark executor, which is created after application submission. 
At this point, H2O starts services, including distributed KV store and memory manager,
and orchestrates them into a cloud. The topology of the created cloud matches the topology of the underlying Spark cluster exactly.

 ![Topology](design-doc/images/Sparkling Water cluster.png)

When H2O services are running, it is possible to create H2O data structures, call H2O algorithms, and transfer values from/to RDD.

---

<a name="Features"></a>
## Features

Sparkling Water provides transparent integration for the H2O engine and its machine learning 
algorithms into the Spark platform, enabling:
 * use of H2O algorithms in Spark workflow
 * transformation between H2O and Spark data structures
 * use of Spark RDDs as input for H2O algorithms
 * transparent execution of Sparkling Water applications on top of Spark


<a name="DataSource"></a> 
### Supported Data Sources
Currently, Sparkling Water can use the following data source types:
 - standard RDD API to load data and transform them into `H2OFrame`
 - H2O API to load data directly into `H2OFrame` from:
   - local file(s)
   - HDFS file(s)
   - S3 file(s)

---
<a name="DataFormat"></a>   
### Supported Data Formats
Sparkling Water can read data stored in the following formats:

 - CSV
 - SVMLight
 - ARFF


---
<a name="DataShare"></a>
### Data Sharing
Sparkling Water enables transformation between different types of Spark `RDD` and H2O's `H2OFrame`, and vice versa.

 ![Data Sharing](design-doc/images/DataShare.png)

When converting from `H2OFrame` to `RDD`, a wrapper is created around the `H2OFrame` to provide an RDD-like API. In this case, no data is duplicated; instead, the data is served directly from then underlying `H2OFrame`.

Converting in the opposite direction (i.e, from Spark `RDD`/`DataFrame` to `H2OFrame`) needs evaluation of data stored in Spark `RDD` and transfer them from RDD storage into `H2OFrame`. However, data stored in `H2OFrame` is heavily compressed. 
