# Well Architected Review

This document captures how well this solution adheres to some of the key requirements of well architected framework for serverless applications.

***OPS 1. How do you understand the health of your Serverless application?***

- [x] **[Required] Understand, analyze and alert on metrics provided out of the box**

  *CloudWatch metrics. Email alerts for Step function failures*
- [] **[Best] Using  application, business, and operations metrics**
- [x] **[Good] Using  distributed tracing and code is instrumented with additional context**

  *x-Ray integration for all Lambda function with custom sub-segments to trace downstream calls*
- [x] **[Good] Structured and  centralized logging**

  *CloudWatch integration*

***OPS 2. How do you approach application lifecycle management?***

- [x] **[Required] Use infrastructure as code and stages isolated in separate environments**

  *SAM to deploy code*
- [x] **[Good] Prototype new features using temporary environments**

  *Code reviewed and tested in internal account*
- [] **[Good] Use a rollout  deployment mechanism**
- [] **[Best] Use CI/CD including automated  testing across separate accounts**
- [x] **[Good] Use configuration management**

  *SAM to deploy code*
- [x] **[Good] Review function runtime deprecation policy**

  *Using latest available Lamba Python runtime (3.8)

***REL 1. How are you regulating inbound request rates?***

- [] **[Required] Control inbound request rates using throttling**

  *Not applicable as this is an on-demand workflow*
- [] **[Good] Use, analyze, and enforce API quotas**

  *Not applicable*
- [x] **[Best] Use mechanisms to protect non-scalable resources**

  *Step function map state concurrency is configurable*

***REL 2. How are you building resiliency into your Serverless application?***

- [x] **[Required] Manage transaction, partial, and intermittent failures**

  *Step functions with saga pattern to handle failures*
- [x] **[Good] Orchestrate long-running transactions**

  *Step functions to handle orchestration*
- [x] **[Required] Manage  duplicate and unwanted events**

  *Workflow is idempotent*
- [] **[Best] Consider scaling  patterns at burst rates**

  *Not applicable. This is an on-demand ETL workflow designed to run once a day, hence no bursts*

***SEC 1: How do you control access to your Serverless API?***

- [x] **[Good] Use an authentication and an authorization mechanism**

  *Lambda and Step function roles scoped based on access needed*
- [] **[Best] Scope access based on identity’s metadata**

  *Lambda and Step function roles scoped based on access needed*
- [] **[Required] Use appropriate endpoint type  and mechanisms to secure access to your API**

  *Not applicable. There is no API*

***SEC 2: How are you managing the security boundaries of your Serverless Application?***

- [x] **[Required] Evaluate and define resource policies**

  *Lambda and Step function roles scoped to resources where possible*
- [] **[Good] Control network traffic at all layers**

  *Not applicable*
- [x] **[Best] Smaller functions require fewer permissions**

  *Application is decomposed to avoid large functions*
- [x] **[Required] Use temporary credentials between resources and components**

  *Using IAM roles for Lambda and Step functions*

***SEC 3: How do you implement Application Security in your workload?***

- [x] **[Required] Review security awareness documents frequently**
- [] **[Good] Implement runtime protection to help prevent against malicious code execution**
- [] **[Best] Automatically review workload’s code dependencies/libraries**
- [] **[Required] Store secrets that are used in  your code securely**

  *Not applicable*
- [] **[Best] Validate inbound events**

  *Not applicable. No dependency on event*

***PERF 1. How have you optimized the performance of your Serverless Application?***

- [x] **[Good] Measure and optimize function startup time**
- [] **[Good] Design your function to take advantage of concurrency via async and stream-based invocations**

  *Not applicable*
- [x] **[Required] Measure, evaluate and select optimum capacity units**

  *Lambda memory chosen to balance cost and performance*
- [x] **[Best] Integrate with managed services directly over functions when possible**

  *Step functions integrates directly with SNS for notification*
- [] **[Good] Optimize access patterns and apply caching where applicable**

  *Not applicable*

***COST 1. How do you optimize your costs?***

- [x] **[Required] Minimize external calls and function code initialization**
- [x] **[Required] Optimize logging output and its retention**

  *Configurable log levels*
- [x] **[Good] Optimize function configuration in order to reduce cost**

  *Lambda memory chosen to balance cost and performance*
- [] **[Best] Use cost-aware usage patterns in code**
