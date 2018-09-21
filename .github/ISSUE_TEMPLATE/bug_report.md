---
name: Bug report
about: Create a report to help us improve!

---

Providing us with the observed and expected behavior definitely helps. Giving us with the following information definitively helps:

- Sparkling Water/PySparkling/RSparkling version
- Hadoop Version & Distribution
- Execution mode `YARN-client`, `YARN-cluster`, standalone, local ..
- YARN logs in case of running on yarn. To collect such a logs you may run `yarn logs -applicationId <application ID>` where the application ID is displayed when Sparkling Water is started
- H2O & Spark logs if not running on YARN. You can find these logs in Spark work directory
- Are you using Windows/Linux/MAC?
- Spark & Sparkling Water configuration including the memory configuration

Please also provide us with the full and minimal reproducible code.
