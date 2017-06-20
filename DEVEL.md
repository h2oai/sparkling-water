# Sparkling Water Development Documentation

This documentation acts only as the table of content and provides links to the actual documentation documents.

- [Typical Use Case](#UseCase)
- [Requirements](#Req)
- [Design](#Design)
    - [Basic Primitives](doc/design/basic_primitives.rst)
    - [Supported Platforms](doc/design/supported_platforms.rst)  
    - [Spark Frame - H2O Frame Mapping](doc/design/spark_h2o_mapping.rst)
    - [Data Sharing Between H2O and Spark](doc/design/data_sharing.rst)
  - [Supported Data Sources](#DataSource)
  - [Supported Data Formats](#DataFormat)
- [Running Sparkling Water](doc/tutorials/run_sparkling_water.rst)
- **Sparkling Water Configuration**
    - [Sparkling Water Configuration Properties](doc/configuration/configuration_properties.rst)
    - [Memory Allocation](doc/configuration/memory_setup.rst)
    - [Sparkling Water Internal Backend Tuning](doc/configuration/internal_backend_tuning.rst)
- **Tutorials**    
    - [Enabling Security](doc/tutorials/security.rst)
    - [Calling H2O Algorithms](doc/tutorials/calling_h2o_algos.rst)
    - Frames Conversions & Creation
        - [Spark Frame - H2O Frame Conversions](doc/tutorials/spark_h2o_conversions.rst)
        - [H2O Frame as Spark's Data Source](doc/tutorials/h2oframe_as_data_source.rst)
        - [Create H2OFrame From an Existing Key](doc/tutorials/h2o_frame_from_key.rst)
    - Logging
        - [Change Sparkling Shell Logging Level](doc/tutorials/change_log_level.rst)
        - [Obtain Sparkling Water Logs](doc/tutorials/obtaining_logs.rst)
- **Testing**
    - [Running Unit Tests](doc/devel/unit_tests.rst)
    - [Integration Tests](doc/devel/integ_tests.rst)
- **Sparkling Water Rest API**
    - [Scala Interpreter REST API Overview](doc/rest_api_scala_endpoints.md)
- [Sparkling Water and Zeppelin](doc/tutorials/use_on_zeppelin.rst)

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

 ![Topology](doc/images/Sparkling Water cluster.png)

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

