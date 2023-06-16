# Set up of the FDTN Infrastructure

## Main needs: 
- Pipeline orchestrator
- Postgres database for anything georelated
- Low cost data storage  
- App hosting
- Computing power for intermittent projects 

We chosed to go with Azure as this is the Cloud Provided our Valencia team is the most familiar with and we should be able to transition it to them as soon as we have some budget 

### Pipeline orchestrator 

Decided to go with Airflow as it's widely used open source tool and the team is already familiar with it. 
There were several options within Azure: 
- Managed airflow within the Azure Data Factory - but too expensive for our limited budget - however could be interesting later on if we scale up 
- Direct deployment via Docker container - tried it and run into several errors as it was more complex 
- Deployment via Kubernetes and Helm chart - this was the easiest and most documented way - possible to directly deploy the official helm chart 
   
