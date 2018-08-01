Change Log
==========

v2.3.10 (2018-08-01)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/10/index.html>`__

-  Bug
        
   -  `SW-903 <https://0xdata.atlassian.net/browse/SW-903>`__ - Automate releases of RSparkling and create release pipeline for this release proccess 
   -  `SW-911 <https://0xdata.atlassian.net/browse/SW-911>`__ - Add missing repository to the documentation
   -  `SW-944 <https://0xdata.atlassian.net/browse/SW-944>`__ - Fix Sphinx gradle plugin, the latest version does not work
   -  `SW-945 <https://0xdata.atlassian.net/browse/SW-945>`__ - Stabilize releasing to Nexus Repository
   -  `SW-953 <https://0xdata.atlassian.net/browse/SW-953>`__ - Do not stop external H2O backend in case of manual start mode
   -  `SW-958 <https://0xdata.atlassian.net/browse/SW-958>`__ - Fix RSparkling README style issues
   -  `SW-959 <https://0xdata.atlassian.net/browse/SW-959>`__ - Fix address for fetching H2O R package in nightly tests
   -  `SW-961 <https://0xdata.atlassian.net/browse/SW-961>`__ - Add option to ignore SPARK_PUBLIC_DNS
   -  `SW-962 <https://0xdata.atlassian.net/browse/SW-962>`__ - Add option which ensures that items in flatfile are translated to IP address
   -  `SW-967 <https://0xdata.atlassian.net/browse/SW-967>`__ - Deprecate old behaviour of mojo pipeline output in SW
                
-  Improvement
        
   -  `SW-233 <https://0xdata.atlassian.net/browse/SW-233>`__ - Warn if user&#39;s h2o in python env is different then the one bundled in pysparkling
   -  `SW-921 <https://0xdata.atlassian.net/browse/SW-921>`__ - Move Rsparkling to Sparkling Water repo
   -  `SW-941 <https://0xdata.atlassian.net/browse/SW-941>`__ - Upgrade Gradle to 4.9
   -  `SW-952 <https://0xdata.atlassian.net/browse/SW-952>`__ - Fix issues when stopping Sparkling Water (Scala) in yarn-cluster mode for external Backend
   -  `SW-957 <https://0xdata.atlassian.net/browse/SW-957>`__ - RSparkling should run tests in both, external and internal mode
   -  `SW-963 <https://0xdata.atlassian.net/browse/SW-963>`__ - Upgrade H2O to 3.20.0.4
   -  `SW-965 <https://0xdata.atlassian.net/browse/SW-965>`__ - Expose port offset in Sparkling Water
   -  `SW-968 <https://0xdata.atlassian.net/browse/SW-968>`__ - Remove confusing message about stopping H2OContext in PySparkling
                
                                
v2.3.9 (2018-07-16)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/9/index.html>`__

-  Bug
        
   -  `SW-898 <https://0xdata.atlassian.net/browse/SW-898>`__ - Issues with HTTP libraries on SPark 2.3
   -  `SW-902 <https://0xdata.atlassian.net/browse/SW-902>`__ - Upgrade Gradle to 4.8.1
   -  `SW-904 <https://0xdata.atlassian.net/browse/SW-904>`__ - Upgrade Mojo2 version to 0.10.7
   -  `SW-908 <https://0xdata.atlassian.net/browse/SW-908>`__ - Exclude Hadoop dependencies as they are provided by Spark
   -  `SW-909 <https://0xdata.atlassian.net/browse/SW-909>`__ - Fix issues when stopping Sparkling Water (Scala) in yarn-cluster mode
   -  `SW-925 <https://0xdata.atlassian.net/browse/SW-925>`__ - Fix missing aposthrope in documentation
   -  `SW-929 <https://0xdata.atlassian.net/browse/SW-929>`__ - Disable temporarily AutoML tests in Sparkling Water
                
-  New Feature
        
   -  `SW-826 <https://0xdata.atlassian.net/browse/SW-826>`__ - Implement Synchronous and Asynchronous Scala cell behaviour
                
-  Improvement
        
   -  `SW-846 <https://0xdata.atlassian.net/browse/SW-846>`__ - Don&#39;t parse types again when passing data to mojo pipeline
   -  `SW-886 <https://0xdata.atlassian.net/browse/SW-886>`__ - Several Scala cell improvements in H2O flow
   -  `SW-887 <https://0xdata.atlassian.net/browse/SW-887>`__ - Make sure that we can use schemes unsupported by H2O in H2O Confoguration
   -  `SW-889 <https://0xdata.atlassian.net/browse/SW-889>`__ - Port AWS preparation scripts into SW codebase
   -  `SW-894 <https://0xdata.atlassian.net/browse/SW-894>`__ - Add support for queuing of Scala cell jobs 
   -  `SW-914 <https://0xdata.atlassian.net/browse/SW-914>`__ - Wrong Spark version in documentation
   -  `SW-917 <https://0xdata.atlassian.net/browse/SW-917>`__ - Dockerize Sparkling Water release pipeline
   -  `SW-919 <https://0xdata.atlassian.net/browse/SW-919>`__ - Clean gradle build with regards to mojo2
   -  `SW-922 <https://0xdata.atlassian.net/browse/SW-922>`__ - Upgrade H2O to 3.20.0.3
   -  `SW-928 <https://0xdata.atlassian.net/browse/SW-928>`__ - Expose AutoML max models
                
-  Docs
        
   -  `SW-878 <https://0xdata.atlassian.net/browse/SW-878>`__ - Add section for using Sparkling Water with AWS
                
                            
v2.3.8 (2018-06-18)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/8/index.html>`__

-  Improvement
        
   -  `SW-885 <https://0xdata.atlassian.net/browse/SW-885>`__ - Upgrade H2O to 3.20.0.2
                
                                
v2.3.7 (2018-06-18)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/7/index.html>`__

-  Bug
        
   -  `SW-858 <https://0xdata.atlassian.net/browse/SW-858>`__ - SVM throws unsupported operations exception on Spark 2.3
   -  `SW-861 <https://0xdata.atlassian.net/browse/SW-861>`__ - Upgrade Gradle to 4.8 (publishing plugin)
   -  `SW-872 <https://0xdata.atlassian.net/browse/SW-872>`__ - Fix reference to local-cluster on download page
   -  `SW-880 <https://0xdata.atlassian.net/browse/SW-880>`__ - Update Hadoop version on download page
   -  `SW-881 <https://0xdata.atlassian.net/browse/SW-881>`__ - Fix Script tests on Dockerized Jenkins infrastructure
   -  `SW-882 <https://0xdata.atlassian.net/browse/SW-882>`__ - Call h2oContext.stop after ham or spam Scala example
   -  `SW-883 <https://0xdata.atlassian.net/browse/SW-883>`__ - Add mising description in publish.gradle
                
-  Improvement
        
   -  `SW-860 <https://0xdata.atlassian.net/browse/SW-860>`__ - Modify the hadoop launch command on download page
   -  `SW-863 <https://0xdata.atlassian.net/browse/SW-863>`__ - Upgrade infrastructure and references to Spark 2.3.1 
   -  `SW-873 <https://0xdata.atlassian.net/browse/SW-873>`__ - Upgrade H2O to 3.20.0.1
   -  `SW-874 <https://0xdata.atlassian.net/browse/SW-874>`__ - Update Mojo2 to 0.10.4
   -  `SW-876 <https://0xdata.atlassian.net/browse/SW-876>`__ - FIx local PySparkling integtest on jenkins infrastracture
   -  `SW-879 <https://0xdata.atlassian.net/browse/SW-879>`__ - Print output of script tests
                
                                
v2.3.6 (2018-06-14)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/6/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/6/index.html>`__

-  Bug
        
   -  `SW-712 <https://0xdata.atlassian.net/browse/SW-712>`__ - Test non-distributed sparkling water tests in docker
   -  `SW-850 <https://0xdata.atlassian.net/browse/SW-850>`__ - Expose methods to get input/output names in H2OMOJOPipelineModel
   -  `SW-859 <https://0xdata.atlassian.net/browse/SW-859>`__ - Print Warning when spark-home is defined on PATH
   -  `SW-862 <https://0xdata.atlassian.net/browse/SW-862>`__ - Create &amp; fix test in PySparkling for named mojo columns
   -  `SW-864 <https://0xdata.atlassian.net/browse/SW-864>`__ - Fix &amp; more readable test
   -  `SW-865 <https://0xdata.atlassian.net/browse/SW-865>`__ - Better Naming of the UDF method to obtain predictions
   -  `SW-869 <https://0xdata.atlassian.net/browse/SW-869>`__ - Add repository to build required by xgboost-predictor
                
-  Story
        
   -  `SW-856 <https://0xdata.atlassian.net/browse/SW-856>`__ - Upgrade Mojo2 to latest version
                
-  Improvement
        
   -  `SW-839 <https://0xdata.atlassian.net/browse/SW-839>`__ - Verify that Spark time column representation can be digested by Mojo2
   -  `SW-848 <https://0xdata.atlassian.net/browse/SW-848>`__ - Document Kerberos on Sparkling Water
   -  `SW-849 <https://0xdata.atlassian.net/browse/SW-849>`__ - Update use from maven on sparkling water download page
   -  `SW-851 <https://0xdata.atlassian.net/browse/SW-851>`__ - Make use of output types when creating Spark DataFrame out of mojo2 predicted values
   -  `SW-852 <https://0xdata.atlassian.net/browse/SW-852>`__ - Create spark UDF used to extract predicted values
   -  `SW-853 <https://0xdata.atlassian.net/browse/SW-853>`__ - Sparkling Water py should require pyspark dependency
   -  `SW-854 <https://0xdata.atlassian.net/browse/SW-854>`__ - Upgrade MojoPipeline to 0.10.0
   -  `SW-855 <https://0xdata.atlassian.net/browse/SW-855>`__ - Upgrade H2O to 3.18.0.11
                
                                
v2.3.5 (2018-05-23)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/5/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/5/index.html>`__

-  Bug
        
   -  `SW-842 <https://0xdata.atlassian.net/browse/SW-842>`__ - Enforce system level properties in SW
                
-  Improvement
        
   -  `SW-845 <https://0xdata.atlassian.net/browse/SW-845>`__ - Upgrade H2O to 3.18.0.10
   -  `SW-847 <https://0xdata.atlassian.net/browse/SW-847>`__ - Remove GA from Sparkling Water
                
                                
v2.3.4 (2018-05-18)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/4/index.html>`__

-  Bug
        
   -  `SW-836 <https://0xdata.atlassian.net/browse/SW-836>`__ - Add support for converting empty dataframe/RDD in Python and Scala to H2OFrame
   -  `SW-841 <https://0xdata.atlassian.net/browse/SW-841>`__ - Remove withCustomCommitsState in pipelines as it&#39;s now duplicating Github 
   -  `SW-843 <https://0xdata.atlassian.net/browse/SW-843>`__ - Fix data obtaining for mojo pipeline
   -  `SW-844 <https://0xdata.atlassian.net/browse/SW-844>`__ - Upgrade Mojo pipeline to 0.9.9
                
                                                    
v2.3.3 (2018-05-15)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/3/index.html>`__

-  Bug
        
   -  `SW-817 <https://0xdata.atlassian.net/browse/SW-817>`__ - Enable running MOJO spark pipeline without H2O init
   -  `SW-825 <https://0xdata.atlassian.net/browse/SW-825>`__ - Local creation of Sparkling Water does not work anymore.
   -  `SW-831 <https://0xdata.atlassian.net/browse/SW-831>`__ - Check shape of H2O frame after the conversion from Spark frame
   -  `SW-834 <https://0xdata.atlassian.net/browse/SW-834>`__ - External Backend stored sparse vector values incorrectly
                
-  Improvement
        
   -  `SW-829 <https://0xdata.atlassian.net/browse/SW-829>`__ - Type checking in PySparkling pipelines
   -  `SW-832 <https://0xdata.atlassian.net/browse/SW-832>`__ - Small refactoring in identifiers
   -  `SW-833 <https://0xdata.atlassian.net/browse/SW-833>`__ - Explicitly set source and target java versions
   -  `SW-837 <https://0xdata.atlassian.net/browse/SW-837>`__ - Upgrade H2O to 3.18.0.9
   -  `SW-838 <https://0xdata.atlassian.net/browse/SW-838>`__ - Upgrade Mojo pipeline dependency to 0.9.8
   -  `SW-840 <https://0xdata.atlassian.net/browse/SW-840>`__ - Add test checking column names and types between spark and mojo2
                
                                
v2.3.2 (2018-05-02)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/2/index.html>`__

-  Bug
        
   -  `SW-574 <https://0xdata.atlassian.net/browse/SW-574>`__ - Process steam handle and use it for connection to external h2o cluster
   -  `SW-822 <https://0xdata.atlassian.net/browse/SW-822>`__ - Require correct colorama version
   -  `SW-823 <https://0xdata.atlassian.net/browse/SW-823>`__ - Fix Windows starting scripts
   -  `SW-824 <https://0xdata.atlassian.net/browse/SW-824>`__ - Fix NPE in mojo pipeline predictions
                
-  New Feature
        
   -  `SW-827 <https://0xdata.atlassian.net/browse/SW-827>`__ - Change color highlight in scala cell as it is too dark
                
-  Improvement
        
   -  `SW-815 <https://0xdata.atlassian.net/browse/SW-815>`__ - Upgrade H2O to 3.18.0.8
   -  `SW-816 <https://0xdata.atlassian.net/browse/SW-816>`__ - Update Mojo2 dependency to one which is compatible with Java7
   -  `SW-818 <https://0xdata.atlassian.net/browse/SW-818>`__ - Spark Pipeline imports do not work in PySparkling
   -  `SW-819 <https://0xdata.atlassian.net/browse/SW-819>`__ - Add ability to convert specific columns to categoricals in Sparkling Water pipelines
   -  `SW-820 <https://0xdata.atlassian.net/browse/SW-820>`__ - Sparkling Water pipelines add duplicate response column to the list of features
                
                                
v2.3.1 (2018-04-19)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/1/index.html>`__

-  Bug
        
   -  `SW-672 <https://0xdata.atlassian.net/browse/SW-672>`__ - Enable using sparkling water maven packages in databricks cloud 
   -  `SW-787 <https://0xdata.atlassian.net/browse/SW-787>`__ - Documentation fixes
   -  `SW-788 <https://0xdata.atlassian.net/browse/SW-788>`__ - Fix Travis tests on Spark 2.3
   -  `SW-790 <https://0xdata.atlassian.net/browse/SW-790>`__ - Add missing seed argument to H2OAutoml pipeline step
   -  `SW-794 <https://0xdata.atlassian.net/browse/SW-794>`__ - Point to proper web-based docs
   -  `SW-796 <https://0xdata.atlassian.net/browse/SW-796>`__ - Use parquet provided by Spark
   -  `SW-797 <https://0xdata.atlassian.net/browse/SW-797>`__ - Automatically update redirect table as part of release pipeline
   -  `SW-806 <https://0xdata.atlassian.net/browse/SW-806>`__ - Fix exporting and importing of pipeline steps and mojo models to and from HDFS
                
-  Improvement
        
   -  `SW-772 <https://0xdata.atlassian.net/browse/SW-772>`__ - Integrate &amp; Test Mojo Pipeline with Sparkling Water
   -  `SW-789 <https://0xdata.atlassian.net/browse/SW-789>`__ - Upgrade H2O to 3.18.0.7
   -  `SW-791 <https://0xdata.atlassian.net/browse/SW-791>`__ - Expose context_path in Sparkling Water
   -  `SW-793 <https://0xdata.atlassian.net/browse/SW-793>`__ - Create additional test verifying that the new light endpoint works as expected
   -  `SW-798 <https://0xdata.atlassian.net/browse/SW-798>`__ - Additional link to documentation
   -  `SW-800 <https://0xdata.atlassian.net/browse/SW-800>`__ - Remove references to Sparkling Water 2.0
   -  `SW-804 <https://0xdata.atlassian.net/browse/SW-804>`__ - Reduce time of H2OAutoml step in pipeline tests to 1 minute
   -  `SW-808 <https://0xdata.atlassian.net/browse/SW-808>`__ - Upgrade to Gradle 4.7
                
                                
v2.3.0 (2018-03-29)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/0/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/0/index.html>`__

-  Bug
        
   -  `SW-696 <https://0xdata.atlassian.net/browse/SW-696>`__ - Intermittent script test issue on external backend
   -  `SW-726 <https://0xdata.atlassian.net/browse/SW-726>`__ - Mark Spark dependencies as provided on artefacts published to maven
   -  `SW-740 <https://0xdata.atlassian.net/browse/SW-740>`__ - Increase timeout for conversion in pyunit test for external cluster
   -  `SW-760 <https://0xdata.atlassian.net/browse/SW-760>`__ - Fix doc artefact publication
   -  `SW-763 <https://0xdata.atlassian.net/browse/SW-763>`__ - Remove support for downloading H2O logs from Spark UI
   -  `SW-766 <https://0xdata.atlassian.net/browse/SW-766>`__ - Fix coding style issue 
   -  `SW-769 <https://0xdata.atlassian.net/browse/SW-769>`__ - Fix import
   -  `SW-770 <https://0xdata.atlassian.net/browse/SW-770>`__ - Fix link to Spark 2.3 in travis tests
   -  `SW-776 <https://0xdata.atlassian.net/browse/SW-776>`__ - sparkling water from maven does not know the stacktrace_collector_interval option
   -  `SW-778 <https://0xdata.atlassian.net/browse/SW-778>`__ - Handle nulls properly in H2OMojoModel
   -  `SW-779 <https://0xdata.atlassian.net/browse/SW-779>`__ - As from Spark 2.3, use H2O ip address to show instead of spark&#39;s one
   -  `SW-783 <https://0xdata.atlassian.net/browse/SW-783>`__ - Make H2OAutoML pipeline tests deterministic by setting the seed
                
-  New Feature
        
   -  `SW-722 <https://0xdata.atlassian.net/browse/SW-722>`__ - [PySparkling] Check for correct data type as part of as_h2o_frame
                
-  Improvement
        
   -  `SW-733 <https://0xdata.atlassian.net/browse/SW-733>`__ - Parametrize pipeline scripts to be able to specify different algorithms
   -  `SW-746 <https://0xdata.atlassian.net/browse/SW-746>`__ - Log chunk layout after the conversion of data to external H2O cluster
   -  `SW-750 <https://0xdata.atlassian.net/browse/SW-750>`__ - Support for Spark 2.3.0
   -  `SW-755 <https://0xdata.atlassian.net/browse/SW-755>`__ - Document GBM Grid Search Pipeline Step
   -  `SW-765 <https://0xdata.atlassian.net/browse/SW-765>`__ - Remove test artefacts from the sparkling-water assembly
   -  `SW-768 <https://0xdata.atlassian.net/browse/SW-768>`__ - Add missing import
   -  `SW-771 <https://0xdata.atlassian.net/browse/SW-771>`__ - Travis edits - no longer need the workaround for JDK7
   -  `SW-773 <https://0xdata.atlassian.net/browse/SW-773>`__ - Don&#39;t use default value for output dir in external backend, it&#39;s not required
   -  `SW-780 <https://0xdata.atlassian.net/browse/SW-780>`__ - Upgrade H2O to 3.18.0.5
                
-  Docs
        
   -  `SW-775 <https://0xdata.atlassian.net/browse/SW-775>`__ - Fix link for documentation on DEVEL.md
                
                            
v2.2.x (2017-08-17)
-------------------

-  Sparkling Water 2.2 brings support of Spark 2.2.
-  For detailed changelog, please read `rel-2.2/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.2/doc/CHANGELOG.rst>`__.

v2.1.x (2017-03-02)
-------------------

-  Sparkling Water 2.1 brings support of Spark 2.1.
-  For detailed changelog, please read `rel-2.1/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.1/doc/CHANGELOG.rst>`__.

v2.0.x (2016-09-26)
-------------------

-  Sparkling Water 2.0 brings support of Spark 2.0.
-  For detailed changelog, please read `rel-2.0/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.0/doc/CHANGELOG.rst>`__.

v1.6.x (2016-03-15)
-------------------

-  Sparkling Water 1.6 brings support of Spark 1.6.
-  For detailed changelog, please read `rel-1.6/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.6/CHANGELOG.md>`__.

v1.5.x (2015-09-28)
-------------------

-  Sparkling Water 1.5 brings support of Spark 1.5.
-  For detailed changelog, please read `rel-1.5/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.5/CHANGELOG.md>`__.

v1.4.x (2015-07-06)
-------------------

-  Sparkling Water 1.4 brings support of Spark 1.4.
-  For detailed changelog, please read `rel-1.4/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.4/CHANGELOG.md>`__.

v1.3.x (2015-05-25)
-------------------

-  Sparkling Water 1.3 brings support of Spark 1.3.
-  For detailed changelog, please read `rel-1.3/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.3/CHANGELOG.md>`__.

v1.2.x (2015-05-18) and older
-----------------------------

-  Sparkling Water 1.2 brings support of Spark 1.2.
-  For detailed changelog, please read `rel-1.2/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.2/CHANGELOG.md>`__.